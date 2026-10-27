pub mod mqtt {
    use crate::homie::{self, Command};
    use crate::modbus;
    use rumqttc::v5::mqttbytes::v5::LastWill;
    use rumqttc::v5::{AsyncClient, Event, EventLoop, Incoming, MqttOptions};
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio::time::Instant;

    /// Start a Sofar HYD inverter to MQTT proxy which exposes a homie interface to the inverter
    #[derive(clap::Parser)]
    pub struct Args {
        #[clap(flatten)]
        modbus: modbus::Args,
        #[clap(flatten)]
        device_args: homie::Args,

        /// The broker, as `mqtt://host:1883?client_id=name` or `mqtts://…` for TLS.
        #[clap(short = 'm', long)]
        mqtt_broker: String,

        /// The user for password authentication with the broker.
        #[clap(short = 'u', long, requires = "mqtt_password")]
        mqtt_user: Option<String>,

        /// The password for password authentication with the broker.
        #[clap(short = 'p', long, requires = "mqtt_user")]
        mqtt_password: Option<String>,

        /// The ID of the bridge device. The inverter's is this and its serial number.
        #[clap(long, default_value = "sofarhyd")]
        device_id: String,
    }

    #[derive(thiserror::Error, Debug)]
    pub enum Error {
        #[error("could not progress the homie device")]
        DeviceStep(#[source] crate::homie::Error),
        #[error("specified device ID {1} is invalid")]
        InvalidDeviceId(#[source] homie5::InvalidHomieIDError, String),
        #[error("MQTT broker address {1} is not valid")]
        InvalidBrokerAddress(#[source] rumqttc::v5::OptionError, String),
        #[error("could not find the inverter")]
        ResolveInverter(#[source] modbus::Error),
        #[error("communication with the inverter has stopped unexpectedly")]
        ModbusTask(#[source] tokio::task::JoinError),
        #[error("communication with the inverter has ended")]
        ModbusEnded,
    }

    fn commands_gone() -> Error {
        Error::DeviceStep(homie::Error::CommandsGone)
    }

    fn reconnect_delay(failures: u32) -> Duration {
        Duration::from_secs(1 << failures.saturating_sub(1).min(5)).min(Duration::from_secs(30))
    }

    /// Polling is what (re)connects.
    async fn poll_at(
        client_loop: &mut EventLoop,
        at: Instant,
    ) -> Result<Event, rumqttc::v5::ConnectionError> {
        tokio::time::sleep_until(at).await;
        client_loop.poll().await
    }

    fn last_will(will: homie5::client::LastWill) -> LastWill {
        let qos = homie::convert_qos(will.qos);
        LastWill::new(will.topic, will.message, qos, will.retain, None)
    }

    #[tokio::main(flavor = "current_thread")]
    #[expect(clippy::result_large_err)]
    pub async fn run(args: Args) -> Result<(), Error> {
        serve(args).await
    }

    async fn serve(args: Args) -> Result<(), Error> {
        let mut mqtt_options = MqttOptions::parse_url(&args.mqtt_broker)
            .map_err(|e| Error::InvalidBrokerAddress(e, args.mqtt_broker))?;
        if let Some((u, p)) =
            args.mqtt_user.as_ref().and_then(|u| Some((u, args.mqtt_password.as_ref()?)))
        {
            mqtt_options.set_credentials(u, p);
        }

        let device_id = args
            .device_id
            .clone()
            .try_into()
            .map_err(|e| Error::InvalidDeviceId(e, args.device_id))?;
        let modbus::Device { handle, task: mut modbus_task } =
            modbus::connect(&args.modbus, args.device_args.poll_plan())
                .await
                .map_err(Error::ResolveInverter)?;

        // The convention has the will of the root device take every device in the tree with it.
        let (protocol, will) =
            homie5::Homie5DeviceProtocol::new(device_id, homie5::HomieDomain::Default);
        mqtt_options.set_last_will(last_will(will));
        let (client, mut client_loop) = AsyncClient::new(mqtt_options, 100);
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let mut device = homie::Device::new(client, protocol, handle, command_rx);
        command_tx.send(Command::Republish).map_err(|_| commands_gone())?;
        let mut failures = 0u32;
        let mut poll_not_before = Instant::now();
        loop {
            // SUBTLE: `step` is not cancellation safe: it may be blocked publishing on the full MQTT
            // request queue, which only `client_loop.poll()` drains, and dropping it would lose publishes.
            let mut step = std::pin::pin!(device.step());
            let step_result = loop {
                let mqtt_event = tokio::select! {
                    biased;
                    result = poll_at(&mut client_loop, poll_not_before) => match result {
                        Ok(event) => event,
                        Err(error) => {
                            let error = &error as &dyn std::error::Error;
                            if failures == 0 {
                                tracing::warn!(error, "cannot reach the MQTT broker, will keep trying");
                            } else {
                                tracing::debug!(error, "could not connect to the MQTT broker");
                            }
                            failures = failures.saturating_add(1);
                            poll_not_before = Instant::now() + reconnect_delay(failures);
                            continue;
                        }
                    },
                    result = &mut modbus_task => return Err(match result {
                        Ok(()) => Error::ModbusEnded,
                        Err(e) => Error::ModbusTask(e),
                    }),
                    result = &mut step => break result,
                };
                let command = match mqtt_event {
                    // The broker has published the last will, and may have lost the retained messages and our
                    // subscriptions.
                    Event::Incoming(Incoming::ConnAck(_)) if failures > 0 => {
                        tracing::info!("connected to the MQTT broker again");
                        failures = 0;
                        Some(Command::Republish)
                    }
                    Event::Incoming(Incoming::Publish(publish)) if publish.retain => {
                        // Sets are never retained, and one that is would be repeated on every
                        // subscription.
                        tracing::warn!(topic = ?publish.topic, "ignoring a retained message");
                        None
                    }
                    Event::Incoming(Incoming::Publish(publish)) => {
                        let command = Command::try_from_mqtt_command(publish);
                        command.inspect_err(|p| tracing::debug!(topic = ?p.topic, "ignored")).ok()
                    }
                    Event::Incoming(
                        Incoming::PubAck(_)
                        | Incoming::PubRec(_)
                        | Incoming::PubRel(_)
                        | Incoming::PubComp(_)
                        | Incoming::SubAck(_)
                        | Incoming::PingResp(_),
                    )
                    | Event::Outgoing(_) => None,
                    Event::Incoming(event) => {
                        tracing::debug!(?event, "incoming mqtt event not handled");
                        None
                    }
                };
                if let Some(command) = command {
                    command_tx.send(command).map_err(|_| commands_gone())?;
                }
            };
            step_result.map_err(Error::DeviceStep)?;
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::inverter::tests::fixture;
        use crate::modbus::fake::{self, Bank, Step};
        use clap::Parser as _;
        use std::net::SocketAddr;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        use tokio::net::TcpListener;

        type Publishes = mpsc::UnboundedReceiver<(usize, String, String)>;

        fn fixture_bank() -> Bank {
            let (bank, sample) = (Bank::default(), fixture());
            for address in 0..0x1200 {
                sample.get(address).into_iter().for_each(|word| bank.set(address, word));
            }
            bank
        }

        /// Hangs up on everything for as long as `present` is not set.
        async fn serve_inverter(present: Arc<AtomicBool>, bank: Bank) -> SocketAddr {
            let (address, _server) = fake::start(move |request| match present.load(SeqCst) {
                true => bank.respond(request),
                false => vec![Step::Close],
            })
            .await;
            address
        }

        fn will_of(body: &[u8]) -> Option<(String, String)> {
            let flags = *body.get(7)?;
            // Protocol name, version, flags, keep alive and then the properties.
            let mut at = 10;
            let skip_properties = |at: &mut usize| -> Option<()> {
                // The properties we deal with are shorter than 128 bytes.
                *at += 1 + usize::from(*body.get(*at)?);
                Some(())
            };
            skip_properties(&mut at)?;
            let string = |at: &mut usize| -> Option<String> {
                let length =
                    usize::from(u16::from_be_bytes([*body.get(*at)?, *body.get(*at + 1)?]));
                let bytes = body.get(*at + 2..*at + 2 + length)?;
                *at += 2 + length;
                Some(String::from_utf8_lossy(bytes).into_owned())
            };
            string(&mut at)?;
            if flags & 0x04 == 0 {
                return None;
            }
            skip_properties(&mut at)?;
            let topic = string(&mut at)?;
            let payload = string(&mut at)?;
            Some((topic, payload))
        }

        fn filters_of(body: &[u8]) -> Vec<String> {
            // The packet ID and the properties, which are shorter than 128 bytes.
            let mut at = 3 + usize::from(body[2]);
            let mut filters = Vec::new();
            while at < body.len() {
                let length = usize::from(u16::from_be_bytes([body[at], body[at + 1]]));
                filters.push(String::from_utf8_lossy(&body[at + 2..at + 2 + length]).into_owned());
                // The options follow.
                at += 3 + length;
            }
            filters
        }

        fn publish_packet(topic: &str, payload: &str) -> Vec<u8> {
            let mut body = Vec::from((topic.len() as u16).to_be_bytes());
            body.extend(topic.as_bytes());
            body.push(0);
            body.extend(payload.as_bytes());
            let mut packet = vec![0x30];
            let mut length = body.len();
            loop {
                let byte = (length & 0x7f) as u8;
                length >>= 7;
                packet.push(if length > 0 { byte | 0x80 } else { byte });
                if length == 0 {
                    break;
                }
            }
            packet.extend(body);
            packet
        }

        struct Broker {
            publishes: Publishes,
            drop: mpsc::UnboundedSender<()>,
            wills: mpsc::UnboundedReceiver<(String, String)>,
            subscribes: mpsc::UnboundedReceiver<Vec<String>>,
            send: mpsc::UnboundedSender<Vec<u8>>,
        }

        fn serve_broker(listener: TcpListener) -> Broker {
            let (publishes_tx, publishes) = mpsc::unbounded_channel();
            let (wills_tx, wills) = mpsc::unbounded_channel();
            let (subscribes_tx, subscribes) = mpsc::unbounded_channel();
            let (drop_tx, mut drop_rx) = mpsc::unbounded_channel();
            let (send, mut send_rx) = mpsc::unbounded_channel::<Vec<u8>>();
            tokio::spawn(async move {
                let mut connection = 0;
                loop {
                    let Ok((socket, _)) = listener.accept().await else { return };
                    let (mut reader, mut socket) = socket.into_split();
                    // Reading a packet is not cancellation safe.
                    let (packets_tx, mut packets) = mpsc::unbounded_channel();
                    tokio::spawn(async move {
                        while let Some(packet) = read_packet(&mut reader).await {
                            let _ = packets_tx.send(packet);
                        }
                    });
                    let mut published = false;
                    loop {
                        let packet = tokio::select! {
                            packet = packets.recv() => packet,
                            _ = drop_rx.recv() => None,
                            Some(packet) = send_rx.recv() => {
                                if socket.write_all(&packet).await.is_err() {
                                    break;
                                }
                                continue;
                            }
                        };
                        let Some((header, body)) = packet else { break };
                        let id = body.get(..2).unwrap_or_default();
                        let reply: Vec<u8> = match header >> 4 {
                            1 => {
                                if let Some(will) = will_of(&body) {
                                    let _ = wills_tx.send(will);
                                }
                                vec![0x20, 3, 0, 0, 0]
                            }
                            3 => {
                                let topic_len = usize::from(u16::from_be_bytes([body[0], body[1]]));
                                let topic = String::from_utf8_lossy(&body[2..2 + topic_len]);
                                let mut payload = 2 + topic_len;
                                let qos = (header >> 1) & 3;
                                if qos > 0 {
                                    payload += 2;
                                }
                                payload += 1 + usize::from(body[payload]);
                                let payload = String::from_utf8_lossy(&body[payload..]);
                                published = true;
                                let _ =
                                    publishes_tx.send((connection, topic.into(), payload.into()));
                                let id = &body[2 + topic_len..];
                                match qos {
                                    1 => vec![0x40, 2, id[0], id[1]],
                                    2 => vec![0x50, 2, id[0], id[1]],
                                    _ => continue,
                                }
                            }
                            8 => {
                                let filters = filters_of(&body);
                                let mut reply =
                                    vec![0x90, 3 + filters.len() as u8, id[0], id[1], 0];
                                reply.extend(filters.iter().map(|_| 0));
                                let _ = subscribes_tx.send(filters);
                                reply
                            }
                            6 => vec![0x70, 2, id[0], id[1]],
                            12 => vec![0xd0, 0],
                            _ => continue,
                        };
                        if socket.write_all(&reply).await.is_err() {
                            break;
                        }
                    }
                    // The connections that have not published anything (the bridge is free to give
                    // up on one before it is done) do not count.
                    connection += usize::from(published);
                }
            });
            Broker { publishes, drop: drop_tx, wills, subscribes, send }
        }

        async fn read_packet(
            socket: &mut (impl tokio::io::AsyncRead + Unpin),
        ) -> Option<(u8, Vec<u8>)> {
            let header = socket.read_u8().await.ok()?;
            let (mut length, mut shift) = (0, 0);
            loop {
                let byte = socket.read_u8().await.ok()?;
                length |= usize::from(byte & 0x7f) << shift;
                shift += 7;
                if byte & 0x80 == 0 {
                    break;
                }
            }
            let mut body = vec![0; length];
            socket.read_exact(&mut body).await.ok()?;
            Some((header, body))
        }

        const BRIDGE: &str = "sofarhyd-test";
        const INVERTER: &str = "sofarhyd-test-sp1es120n12345";

        async fn scenario(
            inverter: SocketAddr,
            extra: &[&str],
            scenario: impl AsyncFnOnce(Broker),
        ) {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let broker = listener.local_addr().unwrap();
            let broker_handle = serve_broker(listener);
            let args = Args::parse_from(
                [
                    "test",
                    "--tcp",
                    &inverter.to_string(),
                    "--mqtt-broker",
                    &format!("mqtt://{broker}?client_id=test"),
                    "--device-id",
                    BRIDGE,
                    "--poll-period",
                    "100ms",
                    "--timeout",
                    "1s",
                    "--request-gap",
                    "1ms",
                    "--unavailable-after",
                    "500ms",
                ]
                .into_iter()
                .chain(extra.iter().copied()),
            );
            tokio::select! {
                result = serve(args) => panic!("the bridge has ended: {result:?}"),
                result = tokio::time::timeout(Duration::from_secs(15), scenario(broker_handle)) => {
                    result.expect("the scenario did not play out");
                }
            }
        }

        async fn until_published(
            publishes: &mut Publishes,
            connection: usize,
            topic: &str,
            payload: &str,
        ) {
            while let Some((from, published, value)) = publishes.recv().await {
                if (from, published.as_str(), value.as_str()) == (connection, topic, payload) {
                    return;
                }
            }
            panic!("the broker has gone");
        }

        async fn until_state(
            publishes: &mut Publishes,
            device: &str,
            connection: usize,
            state: &str,
        ) {
            until_published(publishes, connection, &format!("homie/5/{device}/$state"), state)
                .await;
        }

        #[test]
        fn small_things() {
            let (_, will) = homie5::Homie5DeviceProtocol::new(
                "sofarhyd".to_string().try_into().unwrap(),
                homie5::HomieDomain::Default,
            );
            let will = last_will(will);
            assert_eq!(will.topic, "homie/5/sofarhyd/$state");
            assert_eq!(&will.message[..], b"lost");
            assert!(will.retain);
            assert_eq!(will.qos, rumqttc::v5::mqttbytes::QoS::ExactlyOnce);

            let delays: Vec<_> = (0..9).map(|n| reconnect_delay(n).as_secs()).collect();
            assert_eq!(delays, [1, 1, 2, 4, 8, 16, 30, 30, 30]);
        }

        #[tokio::test]
        async fn the_device_id_is_validated() {
            for id in ["Sofar HYD", ""] {
                let broker = "mqtt://x?client_id=test";
                let argv = ["test", "--tcp", "x", "--mqtt-broker", broker, "--device-id", id];
                let result = serve(Args::parse_from(argv)).await;
                assert!(matches!(result, Err(Error::InvalidDeviceId(..))));
            }
        }

        #[tokio::test]
        async fn the_bridge_and_the_inverter_are_published_with_the_will_of_the_bridge() {
            let inverter = serve_inverter(Arc::new(AtomicBool::new(true)), fixture_bank()).await;
            scenario(inverter, &[], async |mut broker| {
                until_state(&mut broker.publishes, BRIDGE, 0, "ready").await;
                until_state(&mut broker.publishes, INVERTER, 0, "ready").await;
                let will = broker.wills.recv().await.unwrap();
                assert_eq!(will, (format!("homie/5/{BRIDGE}/$state"), "lost".to_string()));
                assert!(broker.subscribes.try_recv().is_err());
            })
            .await;
        }

        #[tokio::test]
        async fn the_devices_are_published_again_when_the_broker_comes_back() {
            let inverter = serve_inverter(Arc::new(AtomicBool::new(true)), fixture_bank()).await;
            scenario(inverter, &["--allow-writes"], async |mut broker| {
                until_state(&mut broker.publishes, INVERTER, 0, "ready").await;
                broker.drop.send(()).unwrap();
                // The new connection starts out with nothing: no last will has been dealt with,
                // no retained messages are there and nothing is subscribed to.
                until_state(&mut broker.publishes, BRIDGE, 1, "ready").await;
                until_state(&mut broker.publishes, INVERTER, 1, "ready").await;
                assert_eq!(broker.wills.recv().await.unwrap().1, "lost");
                assert_eq!(broker.wills.recv().await.unwrap().1, "lost");
                assert_eq!(broker.subscribes.recv().await.unwrap().len(), 13);
                assert_eq!(broker.subscribes.recv().await.unwrap().len(), 13);
            })
            .await;
        }

        #[tokio::test]
        async fn the_inverter_is_disconnected_while_the_gateway_is_gone() {
            let present = Arc::new(AtomicBool::new(true));
            let inverter = serve_inverter(present.clone(), fixture_bank()).await;
            scenario(inverter, &[], async |mut broker| {
                until_state(&mut broker.publishes, INVERTER, 0, "ready").await;
                present.store(false, SeqCst);
                until_state(&mut broker.publishes, INVERTER, 0, "disconnected").await;
                present.store(true, SeqCst);
                until_state(&mut broker.publishes, INVERTER, 0, "ready").await;
            })
            .await;
        }

        #[tokio::test]
        async fn a_set_from_the_broker_is_written_to_the_inverter() {
            let bank = fixture_bank();
            let inverter = serve_inverter(Arc::new(AtomicBool::new(true)), bank.clone()).await;
            scenario(inverter, &["--allow-writes"], async |mut broker| {
                until_state(&mut broker.publishes, INVERTER, 0, "ready").await;
                let subscribed = broker.subscribes.recv().await.unwrap();
                let set = format!("homie/5/{INVERTER}/inverter/enabled/set");
                assert!(subscribed.contains(&set), "{subscribed:?}");
                assert_eq!(bank.get(0x1104), Some(1));
                broker.send.send(publish_packet(&set, "false")).unwrap();
                let value = format!("homie/5/{INVERTER}/inverter/enabled");
                until_published(&mut broker.publishes, 0, &value, "false").await;
                assert_eq!(bank.get(0x1104), Some(0));
            })
            .await;
        }
    }
}

pub mod dump {
    use crate::{homie, inverter, modbus};

    /// Read the inverter once and print every property. Never writes.
    #[derive(clap::Parser)]
    pub struct Args {
        #[clap(flatten)]
        modbus: modbus::Args,
    }

    #[derive(thiserror::Error, Debug)]
    pub enum Error {
        #[error("--allow-writes makes no sense here, this command only reads")]
        WritesAllowed,
        #[error("could not find the inverter")]
        ResolveInverter(#[source] modbus::Error),
        #[error("could not read anything from the inverter")]
        NothingRead,
    }

    pub struct Report {
        pub lines: Vec<String>,
        pub failures: Vec<String>,
        pub read_anything: bool,
    }

    pub async fn collect(args: &Args) -> Result<Report, Error> {
        if args.modbus.allow_writes() {
            return Err(Error::WritesAllowed);
        }
        let blocks = inverter::poll_plan(std::time::Duration::from_secs(1));
        let (snapshot, failed) =
            modbus::read_once(&args.modbus, &blocks).await.map_err(Error::ResolveInverter)?;
        let failures = failed
            .iter()
            .map(|(block, error)| {
                let end = block.start + block.count - 1;
                let mut line = format!("registers {:#06x}..={end:#06x}: {error}", block.start);
                let mut cause = std::error::Error::source(error);
                while let Some(error) = cause {
                    line.push_str(&format!(": {error}"));
                    cause = error.source();
                }
                line
            })
            .collect();
        let lines = homie::dump(&snapshot);
        Ok(Report { lines, failures, read_anything: snapshot.available() })
    }

    #[tokio::main(flavor = "current_thread")]
    pub async fn run(args: Args) -> Result<(), Error> {
        let report = collect(&args).await?;
        report.lines.iter().for_each(|line| println!("{line}"));
        report.failures.iter().for_each(|line| eprintln!("could not read {line}"));
        if report.read_anything { Ok(()) } else { Err(Error::NothingRead) }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::inverter::tests::fixture;
        use crate::modbus::fake::{self, Bank, Req, Step};
        use clap::Parser as _;
        use std::sync::{Arc, Mutex};

        fn args(address: &str, extra: &[&str]) -> Args {
            let argv = ["test", "--tcp", address, "--timeout", "1s", "--request-gap", "1ms"];
            Args::parse_from(argv.into_iter().chain(extra.iter().copied()))
        }

        #[tokio::test]
        async fn writes_are_refused() {
            let result = collect(&args("127.0.0.1:1", &["--allow-writes"])).await;
            assert!(matches!(result, Err(Error::WritesAllowed)));
        }

        #[tokio::test]
        async fn what_the_inverter_has_is_printed_and_nothing_is_written() {
            let (bank, sample) = (Bank::default(), fixture());
            for address in 0..0x1200 {
                sample.get(address).into_iter().for_each(|word| bank.set(address, word));
            }
            let seen = Arc::<Mutex<Vec<Req>>>::default();
            let (record, respond) = (Arc::clone(&seen), bank.responder());
            let (address, _server) = fake::start(move |request| {
                record.lock().unwrap().push(request.clone());
                respond(request)
            })
            .await;
            let report = collect(&args(&address.to_string(), &[])).await.unwrap();
            assert!(report.read_anything && report.failures.is_empty());
            for line in
                ["battery/state-of-charge = 67", "inverter/state = on-grid", "pv-2/voltage = 480.2"]
            {
                assert!(report.lines.contains(&line.to_string()), "{line}: {:?}", report.lines);
            }
            assert!(report.lines.iter().all(|line| line.contains(" = ")));
            let seen = seen.lock().unwrap();
            assert!(!seen.is_empty() && seen.iter().all(|r| !r.is_write() && r.function == 3));
        }

        #[tokio::test]
        async fn what_cannot_be_read_is_reported() {
            let (address, _server) =
                fake::start(|request| vec![Step::Send(request.exception(2))]).await;
            let report = collect(&args(&address.to_string(), &[])).await.unwrap();
            assert!(!report.read_anything);
            assert!(!report.failures.is_empty());
            assert!(report.lines.iter().all(|line| line.ends_with(" = (not read)")));
        }
    }
}
