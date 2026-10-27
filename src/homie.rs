//! The bridge is a nodeless root device so that the last will covers the inverter, whose ID is
//! only known once its serial number has been read.

mod nodes;

use crate::homie::nodes::{Layout, Property};
use crate::inverter::{self, Equipment, MAX_BATTERIES, MAX_PV_INPUTS, Reading};
use crate::modbus::{self, PollBlock, Snapshot};
use futures::StreamExt as _;
use futures::stream::FuturesUnordered;
use homie5::client::{Publish, QoS};
use homie5::device_description::DeviceDescriptionBuilder;
use homie5::{Homie5DeviceProtocol, HomieDeviceStatus, HomieID, PropertyRef};
use rumqttc::v5::mqttbytes::v5::Filter;
use std::future::Future;
use std::pin::Pin;
use tokio::sync::mpsc;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("could not construct the device description")]
    BuildDeviceDescription(#[source] homie5::Homie5ProtocolError),
    #[error("could not construct the topics of the settable properties")]
    ConstructTopicSubscriptions(#[source] homie5::Homie5ProtocolError),
    #[error("could not {1}")]
    Mqtt(#[source] rumqttc::v5::ClientError, String),
    #[error("nothing is going to ask the device to publish anything anymore")]
    CommandsGone,
}

fn parse_period(value: &str) -> Result<humantime::Duration, String> {
    let period: humantime::Duration = value.parse().map_err(|e| format!("{e}"))?;
    if period.is_zero() { Err("the period must not be zero".to_string()) } else { Ok(period) }
}

#[derive(clap::Parser, Clone)]
#[group(id = "homie::Args")]
pub struct Args {
    /// How often the inverter is asked for the state it is in.
    #[clap(long, short = 'r', default_value = "4s", value_parser = parse_period)]
    poll_period: humantime::Duration,
}

impl Args {
    pub(crate) fn poll_plan(&self) -> Vec<PollBlock> {
        inverter::poll_plan(*self.poll_period)
    }
}

/// Everything there could be, if the equipment cannot be made out.
pub(crate) fn dump(snapshot: &Snapshot) -> Vec<String> {
    let equipment = match inverter::equipment(snapshot) {
        Some(Ok(equipment)) => equipment,
        _ => Equipment { pv_inputs: MAX_PV_INPUTS, batteries: MAX_BATTERIES },
    };
    let layout = nodes::layout(equipment, false, &HomieID::new_const("dump"));
    let line = |property: &Property| {
        let value = match (property.read)(snapshot) {
            None => "(not read)".to_string(),
            Some(Ok(value)) => value,
            Some(Err(reason)) => format!("(invalid: {reason})"),
        };
        format!("{}/{} = {value}", property.node, property.id)
    };
    layout.properties.iter().map(line).collect()
}

fn inverter_id(bridge: &HomieID, serial: &str) -> Option<HomieID> {
    let serial = serial.to_lowercase();
    let parts: Vec<_> = serial.split(|c: char| !c.is_ascii_alphanumeric()).collect();
    let suffix = parts.into_iter().filter(|part| !part.is_empty()).collect::<Vec<_>>().join("-");
    if suffix.is_empty() { None } else { HomieID::try_from(format!("{bridge}-{suffix}")).ok() }
}

fn judge(property: &Property, invalid: &mut bool, reading: &Reading) -> Option<String> {
    let (node_id, prop_id) = (&property.node, &property.id);
    match reading {
        None => None,
        Some(Ok(value)) => {
            if std::mem::take(invalid) {
                tracing::info!(%node_id, %prop_id, "the value is valid again");
            }
            Some(value.clone())
        }
        Some(Err(reason)) => {
            if !std::mem::replace(invalid, true) {
                let message =
                    "the inverter has reported an invalid value, the last valid one stays";
                tracing::warn!(%node_id, %prop_id, %reason, "{message}");
            }
            None
        }
    }
}

fn chain(error: &dyn std::error::Error) -> String {
    let mut text = error.to_string();
    let mut cause = error.source();
    while let Some(error) = cause {
        text.push_str(": ");
        text.push_str(&error.to_string());
        cause = error.source();
    }
    text
}

struct Inverter {
    protocol: Homie5DeviceProtocol,
    state: Option<HomieDeviceStatus>,
    layout: Option<Layout>,
    published: Vec<Option<String>>,
    invalid: Vec<bool>,
    /// What the `layout` was made for.
    equipment: Option<Equipment>,
    invalid_equipment: bool,
}

impl Inverter {
    fn new(protocol: Homie5DeviceProtocol) -> Self {
        Inverter {
            protocol,
            state: None,
            layout: None,
            published: Vec::new(),
            invalid: Vec::new(),
            equipment: None,
            invalid_equipment: false,
        }
    }

    fn target_state(&self, available: bool) -> HomieDeviceStatus {
        match &self.layout {
            Some(_) if available => HomieDeviceStatus::Ready,
            Some(_) => HomieDeviceStatus::Disconnected,
            None => HomieDeviceStatus::Init,
        }
    }
}

struct Observed {
    available: bool,
    layout: Option<Layout>,
    readings: Vec<Reading>,
}

struct Completed {
    device: HomieID,
    node: HomieID,
    property: HomieID,
    value: String,
    result: Result<(), String>,
}

async fn carry_out(modbus: modbus::Handle, write: inverter::Write) -> Result<(), String> {
    let read_back = modbus.write(write.start, write.words.clone()).await.map_err(|e| chain(&e))?;
    write.taken(&read_back).map_err(|reason| reason.to_string())
}

type Write = Pin<Box<dyn Future<Output = Completed>>>;

pub(crate) struct Device {
    mqtt: rumqttc::v5::AsyncClient,
    bridge: Homie5DeviceProtocol,
    modbus: modbus::Handle,
    commands: mpsc::UnboundedReceiver<Command>,
    published: bool,
    inverter: Option<Inverter>,
    writes: FuturesUnordered<Write>,
}

impl Device {
    pub(crate) fn new(
        mqtt: rumqttc::v5::AsyncClient,
        bridge: Homie5DeviceProtocol,
        modbus: modbus::Handle,
        commands: mpsc::UnboundedReceiver<Command>,
    ) -> Self {
        Self {
            mqtt,
            bridge,
            modbus,
            commands,
            published: false,
            inverter: None,
            writes: FuturesUnordered::new(),
        }
    }

    async fn publish(&self, message: Publish) -> Result<(), Error> {
        let topic = message.topic.clone();
        let (qos, retain) = (convert_qos(message.qos), message.retain);
        let published = self.mqtt.publish(message.topic, qos, retain, message.payload).await;
        published.map_err(|e| Error::Mqtt(e, format!("publish {topic}")))
    }

    async fn publish_inverter_state(&mut self, state: HomieDeviceStatus) -> Result<(), Error> {
        let Some(inverter) = &mut self.inverter else { return Ok(()) };
        inverter.state = Some(state);
        let message = inverter.protocol.publish_state(state);
        self.publish(message).await
    }

    async fn publish_bridge(&self) -> Result<(), Error> {
        let children = self.inverter.iter().map(|inverter| inverter.protocol.id().clone());
        let description = DeviceDescriptionBuilder::new().name("sofarhyd2mqtt");
        let description = children.fold(description, |d, child| d.add_child(child));
        self.publish(self.bridge.publish_state(HomieDeviceStatus::Init)).await?;
        let description = self.bridge.publish_description(&description.build());
        self.publish(description.map_err(Error::BuildDeviceDescription)?).await?;
        self.publish(self.bridge.publish_state(HomieDeviceStatus::Ready)).await
    }

    fn current_inverter_id(&self) -> Option<HomieID> {
        let Some(Ok(serial)) = inverter::serial_number(&self.modbus.snapshot()) else {
            return None;
        };
        inverter_id(self.bridge.id(), &serial)
    }

    /// The inverter is described anew if its inputs are set up differently.
    fn observe(&mut self) -> Observed {
        let snapshot = self.modbus.snapshot();
        let available = snapshot.available();
        let Some(inverter) = &mut self.inverter else {
            return Observed { available, layout: None, readings: Vec::new() };
        };
        let mut layout = None;
        let equipment = inverter::equipment(&snapshot).filter(|_| inverter::is_complete(&snapshot));
        match equipment {
            Some(Ok(equipment)) if inverter.equipment != Some(equipment) => {
                inverter.equipment = Some(equipment);
                inverter.invalid_equipment = false;
                let writes = self.modbus.writes_allowed();
                layout = Some(nodes::layout(equipment, writes, self.bridge.id()));
            }
            Some(Err(reason)) if !std::mem::replace(&mut inverter.invalid_equipment, true) => {
                tracing::warn!(%reason, "the inputs of the inverter make no sense");
            }
            _ => {}
        }
        let current = layout.as_ref().or(inverter.layout.as_ref());
        let readings = current
            .map(|layout| layout.properties.iter().map(|p| (p.read)(&snapshot)).collect())
            .unwrap_or_default();
        Observed { available, layout, readings }
    }

    /// With `republish` everything is published again, otherwise only what has changed.
    async fn update(&mut self, republish: bool) -> Result<(), Error> {
        if !republish && !self.published {
            return Ok(());
        }
        if republish {
            self.published = true;
        }
        let current = self.inverter.as_ref().map(|inverter| inverter.protocol.id());
        let other = self.current_inverter_id().filter(|id| Some(id) != current);
        let switched = other.is_some();
        if let Some(id) = other {
            tracing::info!(device_id = %id, "the serial number of the inverter is known");
            self.inverter = Some(Inverter::new(self.bridge.clone_for_child(id)));
        }
        if republish || switched {
            // The children first.
            self.publish_inverter_state(HomieDeviceStatus::Init).await?;
            self.publish_bridge().await?;
        }
        let mut full = republish || switched;
        if self.inverter.is_none() {
            return Ok(());
        }
        let Observed { available, layout, readings } = self.observe();
        if let Some(layout) = layout {
            let Some(inverter) = &mut self.inverter else { return Ok(()) };
            tracing::info!(
                equipment = ?inverter.equipment,
                properties = layout.properties.len(),
                "describing the inverter"
            );
            inverter.published = vec![None; layout.properties.len()];
            inverter.invalid = vec![false; layout.properties.len()];
            inverter.layout = Some(layout);
            full = true;
        } else if self.inverter.as_ref().is_some_and(|inverter| inverter.layout.is_none()) {
            return Ok(());
        }
        if full {
            self.publish_inverter(&readings, available).await
        } else {
            self.publish_changes(&readings, available).await
        }
    }

    /// `ready` is not to be had before subscribing.
    async fn publish_inverter(
        &mut self,
        readings: &[Reading],
        available: bool,
    ) -> Result<(), Error> {
        let in_init = |inverter: &Inverter| inverter.state == Some(HomieDeviceStatus::Init);
        if self.inverter.as_ref().is_some_and(|inverter| !in_init(inverter)) {
            self.publish_inverter_state(HomieDeviceStatus::Init).await?;
        }
        let Some(inverter) = &self.inverter else { return Ok(()) };
        let Some(layout) = &inverter.layout else { return Ok(()) };
        let description = inverter.protocol.publish_description(&layout.description);
        self.publish(description.map_err(Error::BuildDeviceDescription)?).await?;
        self.publish_values(readings, |_| true).await?;
        self.subscribe().await?;
        let Some(inverter) = &self.inverter else { return Ok(()) };
        self.publish_inverter_state(inverter.target_state(available)).await
    }

    async fn subscribe(&self) -> Result<(), Error> {
        let Some(inverter) = &self.inverter else { return Ok(()) };
        let Some(layout) = &inverter.layout else { return Ok(()) };
        let subscriptions = inverter
            .protocol
            .subscribe_props(&layout.description)
            .map_err(Error::ConstructTopicSubscriptions)?;
        let filters: Vec<_> = subscriptions
            .map(|subscription| Filter::new(subscription.topic, convert_qos(subscription.qos)))
            .collect();
        // SUBTLE: rumqttc balks at an empty list of subscriptions.
        if filters.is_empty() {
            return Ok(());
        }
        let subscribed = self.mqtt.subscribe_many(filters).await;
        subscribed.map_err(|e| Error::Mqtt(e, "subscribe to the settable properties".to_string()))
    }

    async fn publish_changes(
        &mut self,
        readings: &[Reading],
        available: bool,
    ) -> Result<(), Error> {
        self.publish_values(readings, |_| false).await?;
        // `ready` is to have current values.
        let Some(inverter) = &self.inverter else { return Ok(()) };
        let state = inverter.target_state(available);
        if inverter.state != Some(state) {
            self.publish_inverter_state(state).await?;
            tracing::info!(%state, "the state of the inverter has changed");
        }
        Ok(())
    }

    /// An invalid value is not published: the last valid one stays.
    async fn publish_values(
        &mut self,
        readings: &[Reading],
        force: impl Fn(usize) -> bool,
    ) -> Result<(), Error> {
        let Some(inverter) = &mut self.inverter else { return Ok(()) };
        let Some(layout) = &inverter.layout else { return Ok(()) };
        let mut messages = Vec::new();
        let properties = layout.properties.iter().zip(readings);
        for (idx, ((property, reading), invalid)) in
            properties.zip(inverter.invalid.iter_mut()).enumerate()
        {
            let published = inverter.published.get(idx).and_then(|p| p.as_ref());
            let valid = judge(property, invalid, reading);
            let Some(value) = valid.or_else(|| published.cloned()) else { continue };
            if force(idx) || published != Some(&value) {
                let message =
                    inverter.protocol.publish_value(&property.node, &property.id, &value, true);
                messages.push((idx, message, value));
            }
        }
        for (idx, message, value) in messages {
            self.publish(message).await?;
            let published = self.inverter.as_mut().and_then(|i| i.published.get_mut(idx));
            if let Some(published) = published {
                *published = Some(value);
            }
        }
        Ok(())
    }

    async fn publish_property(&mut self, idx: usize) -> Result<(), Error> {
        let Some(layout) = self.inverter.as_ref().and_then(|inverter| inverter.layout.as_ref())
        else {
            return Ok(());
        };
        let mut readings = vec![None; layout.properties.len()];
        readings[idx] = (layout.properties[idx].read)(&self.modbus.snapshot());
        self.publish_values(&readings, |at| at == idx).await
    }

    async fn handle_command(&mut self, command: Command) -> Result<(), Error> {
        let (property, value) = match command {
            Command::Republish => return self.update(true).await,
            Command::Set { property, value } => (property, value),
        };
        let Some(inverter) = &self.inverter else {
            tracing::debug!(?property, "set ignored, there is no inverter yet");
            return Ok(());
        };
        if inverter.state != Some(HomieDeviceStatus::Ready) {
            tracing::debug!(?property, "set ignored, the inverter is not ready");
            return Ok(());
        }
        let (device_id, node_id, prop_id) =
            (property.device_id(), property.node_id(), property.prop_id());
        if device_id != inverter.protocol.id() {
            tracing::warn!(%device_id, "ignoring a set for a device that is not the inverter");
            return Ok(());
        }
        let Some(layout) = &inverter.layout else { return Ok(()) };
        let Some(idx) = layout.find(node_id, prop_id) else {
            tracing::warn!(%node_id, %prop_id, "ignoring a set for a property that is unknown");
            return Ok(());
        };
        let Some(write) = &layout.properties[idx].write else {
            tracing::warn!(%node_id, %prop_id, "ignoring a set for a property that cannot be set");
            return Ok(());
        };
        let planned = write(&self.modbus.snapshot(), &value);
        let write = match planned {
            Ok(planned) => planned,
            Err(reason) => {
                tracing::warn!(%node_id, %prop_id, %reason, value, "the property cannot be set to this");
                return self.publish_property(idx).await;
            }
        };
        let (device, node, property) = (device_id.clone(), node_id.clone(), prop_id.clone());
        let modbus = self.modbus.clone();
        self.writes.push(Box::pin(async move {
            let result = carry_out(modbus, write).await;
            Completed { device, node, property, value, result }
        }));
        Ok(())
    }

    async fn handle_completed(&mut self, completed: Completed) -> Result<(), Error> {
        let Completed { device, node: node_id, property: prop_id, value, result } = completed;
        if let Err(error) = &result {
            tracing::error!(%node_id, %prop_id, error, "could not set the property");
        }
        let Some(inverter) = &self.inverter else { return Ok(()) };
        let Some(layout) = inverter.layout.as_ref().filter(|_| inverter.protocol.id() == &device)
        else {
            return Ok(());
        };
        let Some(idx) = layout.find(&node_id, &prop_id) else { return Ok(()) };
        if layout.properties[idx].retained {
            // The registers have been read back by now: the snapshot has what the inverter took.
            self.publish_property(idx).await
        } else if result.is_ok() {
            self.publish(inverter.protocol.publish_value(&node_id, &prop_id, &value, false)).await
        } else {
            Ok(())
        }
    }

    /// Not cancellation safe: dropping the future in the middle of handling an event can lose
    /// some of the publishes.
    pub(crate) async fn step(&mut self) -> Result<(), Error> {
        tokio::select! {
            () = self.modbus.changed() => self.update(false).await,
            command = self.commands.recv() => match command {
                Some(command) => self.handle_command(command).await,
                None => Err(Error::CommandsGone),
            },
            Some(completed) = self.writes.next(), if !self.writes.is_empty() => {
                self.handle_completed(completed).await
            }
        }
    }
}

pub fn convert_qos(homie: QoS) -> rumqttc::v5::mqttbytes::QoS {
    match homie {
        QoS::AtMostOnce => rumqttc::v5::mqttbytes::QoS::AtMostOnce,
        QoS::AtLeastOnce => rumqttc::v5::mqttbytes::QoS::AtLeastOnce,
        QoS::ExactlyOnce => rumqttc::v5::mqttbytes::QoS::ExactlyOnce,
    }
}

#[derive(Debug)]
pub(crate) enum Command {
    Set { property: PropertyRef, value: String },
    Republish,
}

impl Command {
    #[expect(clippy::result_large_err)]
    pub(crate) fn try_from_mqtt_command(
        msg: rumqttc::v5::mqttbytes::v5::Publish,
    ) -> Result<Self, rumqttc::v5::mqttbytes::v5::Publish> {
        let Ok(topic) = str::from_utf8(&msg.topic) else {
            return Err(msg);
        };
        match homie5::parse_mqtt_message(topic, &msg.payload) {
            Ok(homie5::Homie5Message::PropertySet { property, set_value }) => {
                Ok(Self::Set { property, value: set_value })
            }
            _ => Err(msg),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inverter::tests::fixture;
    use crate::modbus::fake::{self, Bank, Req};
    use clap::Parser as _;
    use rumqttc::v5::Request;
    use serde_json::{Value, json};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    macro_rules! bridge {
        ($path:literal) => {
            concat!("homie/5/sofarhyd", $path)
        };
    }
    /// The serial number of the inverter of the fixture is `SP1ES120N12345`.
    macro_rules! inverter {
        ($path:literal) => {
            concat!("homie/5/sofarhyd-sp1es120n12345", $path)
        };
    }

    #[derive(clap::Parser)]
    struct Cli {
        #[clap(flatten)]
        modbus: modbus::Args,
        #[clap(flatten)]
        homie: Args,
    }

    fn cli(extra: &[&str]) -> Cli {
        Cli::parse_from(["test", "--tcp", "x"].into_iter().chain(extra.iter().copied()))
    }

    /// A device that is connected to nothing: what it publishes is queued for the test to see.
    struct Rig {
        device: Device,
        requests: flume::Receiver<Request>,
        commands: mpsc::UnboundedSender<Command>,
    }

    struct Msgs(Vec<(String, String)>);

    impl Msgs {
        fn of(&self, topic: &str) -> Vec<&str> {
            self.0.iter().filter(|(t, _)| t == topic).map(|(_, p)| p.as_str()).collect()
        }

        fn position(&self, topic: &str, payload: &str) -> usize {
            let found = self.0.iter().position(|(t, p)| t == topic && p == payload);
            found.unwrap_or_else(|| panic!("{topic} = {payload:?} not published: {:?}", self.0))
        }

        /// The last payload published to the topic.
        fn value(&self, topic: &str) -> &str {
            self.of(topic).last().unwrap_or_else(|| panic!("{topic} not published"))
        }

        fn pairs(&self) -> Vec<(&str, &str)> {
            self.0.iter().map(|(t, p)| (t.as_str(), p.as_str())).collect()
        }

        fn deleted(&self) -> Vec<&str> {
            self.0.iter().filter(|(_, p)| p.is_empty()).map(|(t, _)| t.as_str()).collect()
        }

        fn description(&self, device: &str) -> Value {
            serde_json::from_str(self.of(&format!("{device}/$description"))[0]).unwrap()
        }
    }

    fn publishes(requests: &[Request]) -> Msgs {
        let publishes = requests.iter().filter_map(|request| match request {
            Request::Publish(p) => Some((
                String::from_utf8(p.topic.to_vec()).unwrap(),
                String::from_utf8(p.payload.to_vec()).unwrap(),
            )),
            _ => None,
        });
        Msgs(publishes.collect())
    }

    fn subscriptions(requests: &[Request]) -> Vec<String> {
        let subscribed = requests.iter().filter_map(|request| match request {
            Request::Subscribe(s) => Some(s.filters.iter().map(|f| f.path.clone())),
            _ => None,
        });
        subscribed.flatten().collect()
    }

    fn keys(value: &Value) -> Vec<&str> {
        let mut keys: Vec<_> = value.as_object().unwrap().keys().map(String::as_str).collect();
        keys.sort_unstable();
        keys
    }

    /// A set of the property as a controller would publish it.
    fn set_command(topic: &str, value: &str) -> Command {
        let qos = rumqttc::v5::mqttbytes::QoS::AtMostOnce;
        let publish = rumqttc::v5::mqttbytes::v5::Publish::new(topic, qos, value.to_string(), None);
        Command::try_from_mqtt_command(publish).expect("a set")
    }

    impl Rig {
        fn with_handle(handle: modbus::Handle) -> Rig {
            let (requests_tx, requests) = flume::unbounded();
            let client = rumqttc::v5::AsyncClient::from_senders(requests_tx);
            let id = HomieID::new_const("sofarhyd");
            let (protocol, _) = Homie5DeviceProtocol::new(id, homie5::HomieDomain::Default);
            let (commands, commands_rx) = mpsc::unbounded_channel();
            let device = Device::new(client, protocol, handle, commands_rx);
            Rig { device, requests, commands }
        }

        fn new(snapshot: Snapshot) -> Rig {
            Rig::with_handle(modbus::Handle::detached(snapshot))
        }

        /// The inverter is there, and everything has been read.
        fn ready() -> Rig {
            let rig = Rig::new(fixture());
            rig.available(true);
            rig
        }

        fn empty() -> Rig {
            Rig::new(Snapshot::new())
        }

        fn set(&self, address: u16, word: u16) {
            self.device.modbus.snapshot_mut().set(address, word);
        }

        fn available(&self, available: bool) {
            self.device.modbus.snapshot_mut().set_available(available);
        }

        fn read_sample(&self, addresses: std::ops::Range<u16>) {
            let sample = fixture();
            for address in addresses {
                self.set(address, sample.get(address).unwrap());
            }
        }

        fn set_serial(&self, serial: &str) {
            let mut bytes = serial.as_bytes().to_vec();
            bytes.resize(14, 0);
            for (i, word) in bytes.chunks(2).enumerate() {
                self.set(0x0445 + i as u16, u16::from_be_bytes([word[0], word[1]]));
            }
        }

        async fn update(&mut self) {
            self.device.update(false).await.unwrap();
        }

        async fn republish(&mut self) {
            self.device.update(true).await.unwrap();
        }

        /// What the MQTT client was asked to do since the last time.
        fn requests(&self) -> Vec<Request> {
            self.requests.try_iter().collect()
        }

        fn published(&self) -> Msgs {
            publishes(&self.requests())
        }

        fn inverter(&self) -> &Inverter {
            self.device.inverter.as_ref().unwrap()
        }

        /// Handle the command, and everything that follows from it.
        async fn run(&mut self, command: Command) {
            self.commands.send(command).unwrap();
            let settled = async {
                loop {
                    self.device.step().await.unwrap();
                    if self.device.writes.is_empty() && self.device.commands.is_empty() {
                        return;
                    }
                }
            };
            tokio::time::timeout(Duration::from_secs(5), settled).await.expect("settled");
        }
    }

    #[test]
    fn the_id_of_the_inverter_is_made_of_the_serial_number() {
        let bridge = HomieID::new_const("sofarhyd");
        for (serial, id) in [
            ("SP1ES120N12345", Some("sofarhyd-sp1es120n12345")),
            ("AB 12/_3", Some("sofarhyd-ab-12-3")),
            ("  --Ab--  ", Some("sofarhyd-ab")),
            ("a.b.c.", Some("sofarhyd-a-b-c")),
            ("", None),
            (" -_. ", None),
            ("ÄÖ", None),
        ] {
            assert_eq!(inverter_id(&bridge, serial).as_ref().map(HomieID::as_str), id, "{serial}");
        }
    }

    #[test]
    fn the_arguments_are_checked() {
        let blocks = |extra: &[&str]| {
            let plan = cli(extra).homie.poll_plan();
            plan.iter().map(|b| (b.start, b.count, b.period.as_secs())).collect::<Vec<_>>()
        };
        assert!(blocks(&[]).contains(&(0x0584, 12, 4)));
        assert!(blocks(&["-r", "2s"]).contains(&(0x0604, 28, 2)));
        let parse = |extra: &[&str]| {
            Cli::try_parse_from(["test", "--tcp", "x"].into_iter().chain(extra.iter().copied()))
        };
        assert!(parse(&["--poll-period", "0s"]).is_err());
    }

    #[tokio::test]
    async fn the_devices_are_published_children_first() {
        let mut rig = Rig::ready();
        rig.republish().await;
        let requests = rig.requests();
        let msgs = publishes(&requests);

        let bridge = msgs.description(bridge!(""));
        assert_eq!(bridge["children"], json!(["sofarhyd-sp1es120n12345"]));
        assert_eq!(bridge["name"], "sofarhyd2mqtt");
        assert!(bridge.get("nodes").is_none() && bridge.get("root").is_none());
        assert_eq!(msgs.of(bridge!("/$state")), ["init", "ready"]);

        let inverter = msgs.description(inverter!(""));
        assert_eq!(
            (&inverter["root"], &inverter["name"]),
            (&json!("sofarhyd"), &json!("Sofar HYD inverter"))
        );
        assert!(inverter.get("children").is_none());
        let nodes = &inverter["nodes"];
        let ids = ["battery", "energy", "eps", "grid", "identification", "inverter", "l1"];
        let more = ["l2", "l3", "load", "passive", "pv", "pv-1", "pv-2"];
        assert_eq!(keys(nodes), [&ids[..], &more[..]].concat());
        assert_eq!(nodes["battery"]["properties"]["state-of-charge"]["format"], "0:100");
        assert_eq!(nodes["l1"]["properties"]["voltage"]["unit"], "V");
        assert_eq!(nodes["inverter"]["properties"]["state"]["datatype"], "enum");
        assert_eq!(nodes["inverter"]["properties"]["faults"]["datatype"], "json");
        assert_ne!(nodes["inverter"]["properties"]["storage-mode"]["settable"], true);
        let mut described = 0;
        for node in keys(nodes) {
            for property in keys(&nodes[node]["properties"]) {
                described += 1;
                msgs.value(&format!("{}/{node}/{property}", inverter!("")));
            }
        }
        assert_eq!(described, 76);
        assert_eq!(msgs.value(inverter!("/battery/state-of-charge")), "67");
        assert_eq!(msgs.value(inverter!("/inverter/state")), "on-grid");

        let init = msgs.position(inverter!("/$state"), "init");
        let bridge_init = msgs.position(bridge!("/$state"), "init");
        let ready = msgs.position(inverter!("/$state"), "ready");
        let described = msgs.0.iter().position(|(t, _)| t == inverter!("/$description")).unwrap();
        assert!(init < bridge_init && bridge_init < ready);
        assert!(init < described && described < ready);
        assert_eq!(msgs.of(inverter!("/$state")), ["init", "ready"]);
        assert!(msgs.0.iter().all(|(t, _)| !t.ends_with("/$target") && !t.contains("/$alert")));

        assert!(subscriptions(&requests).is_empty());
        rig.update().await;
        assert!(rig.requests().is_empty());
        rig.republish().await;
        assert_eq!(publishes(&rig.requests()).pairs(), msgs.pairs());

        let mut rig = Rig::empty();
        rig.republish().await;
        let msgs = rig.published();
        assert_eq!(msgs.of(bridge!("/$state")), ["init", "ready"]);
        assert!(msgs.description(bridge!(""))["children"].is_null());
        assert!(!msgs.0.iter().any(|(t, _)| t.contains("sofarhyd-")));
    }

    #[tokio::test]
    async fn values_are_published_only_when_they_change() {
        let mut rig = Rig::ready();
        rig.republish().await;
        rig.requests();

        // The same registers again, and one that nothing is made of.
        rig.update().await;
        rig.set(0x0425, 5);
        rig.update().await;
        assert!(rig.requests().is_empty());

        rig.set(0x0485, 300);
        rig.set(0x0684 + 1, 1300);
        rig.update().await;
        let expected = [
            (inverter!("/inverter/output-power"), "3000"),
            (inverter!("/energy/solar-today"), "13.00"),
        ];
        assert_eq!(rig.published().pairs(), expected);
        assert_eq!(rig.inverter().state, Some(HomieDeviceStatus::Ready));
    }

    #[tokio::test]
    async fn invalid_values_are_not_published_and_the_last_valid_one_stays() {
        let mut rig = Rig::ready();
        rig.republish().await;
        rig.requests();
        let invalid = |rig: &Rig| rig.inverter().invalid.iter().filter(|invalid| **invalid).count();

        // 101 %, and a state that is not one. A different value, in the same update, is.
        rig.set(0x0608, 101);
        rig.set(0x0404, 99);
        rig.set(0x0485, 300);
        rig.update().await;
        assert_eq!(rig.published().pairs(), [(inverter!("/inverter/output-power"), "3000")]);
        assert_eq!(rig.inverter().state, Some(HomieDeviceStatus::Ready));
        assert_eq!(invalid(&rig), 2);

        rig.set(0x0608, 102);
        rig.update().await;
        assert!(rig.requests().is_empty());

        rig.republish().await;
        let msgs = rig.published();
        assert_eq!(msgs.value(inverter!("/battery/state-of-charge")), "67");
        assert_eq!(msgs.value(inverter!("/inverter/state")), "on-grid");
        assert_eq!(msgs.value(inverter!("/inverter/output-power")), "3000");

        rig.set(0x0608, 66);
        rig.update().await;
        assert_eq!(rig.published().pairs(), [(inverter!("/battery/state-of-charge"), "66")]);
        assert_eq!(invalid(&rig), 1);
    }

    #[tokio::test]
    async fn the_inverter_is_init_until_it_can_be_described() {
        let mut rig = Rig::empty();
        rig.update().await;
        assert!(rig.requests().is_empty());
        rig.republish().await;
        assert!(rig.published().of(inverter!("/$state")).is_empty());

        for word in [0, 0x2020, 0x0102, 0x2d2d] {
            (0x0445..0x044C).for_each(|address| rig.set(address, word));
            rig.available(true);
            rig.update().await;
            assert!(rig.requests().is_empty() && rig.device.inverter.is_none());
        }

        rig.read_sample(0x0445..0x045D);
        rig.update().await;
        let msgs = rig.published();
        assert_eq!(msgs.of(inverter!("/$state")), ["init"]);
        assert_eq!(msgs.of(bridge!("/$state")), ["init", "ready"]);
        assert_eq!(msgs.description(bridge!(""))["children"], json!(["sofarhyd-sp1es120n12345"]));
        assert!(msgs.of(inverter!("/$description")).is_empty());

        rig.read_sample(0x0404..0x0432);
        rig.update().await;
        assert!(rig.requests().is_empty());

        *rig.device.modbus.snapshot_mut() = fixture();
        rig.available(true);
        rig.update().await;
        let msgs = rig.published();
        assert_eq!(msgs.of(inverter!("/$state")), ["ready"]);
        assert_eq!(msgs.value(inverter!("/battery/state-of-charge")), "67");
        assert!(msgs.of(bridge!("/$state")).is_empty());
    }

    #[tokio::test]
    async fn the_inverter_follows_the_availability_of_the_inverter() {
        let mut rig = Rig::ready();
        rig.republish().await;
        rig.requests();

        rig.available(false);
        rig.update().await;
        assert_eq!(rig.published().pairs(), [(inverter!("/$state"), "disconnected")]);
        rig.update().await;
        assert!(rig.requests().is_empty());

        rig.set(0x0608, 68);
        rig.available(true);
        rig.update().await;
        let expected =
            [(inverter!("/battery/state-of-charge"), "68"), (inverter!("/$state"), "ready")];
        assert_eq!(rig.published().pairs(), expected);
        rig.available(false);
        rig.update().await;
        rig.available(true);
        rig.update().await;
        let expected = [(inverter!("/$state"), "disconnected"), (inverter!("/$state"), "ready")];
        assert_eq!(rig.published().pairs(), expected);

        rig.available(false);
        rig.republish().await;
        let msgs = rig.published();
        assert_eq!(msgs.of(inverter!("/$state")), ["init", "disconnected"]);
        assert_eq!(msgs.value(inverter!("/battery/state-of-charge")), "68");

        let mut rig = Rig::new(fixture());
        rig.commands.send(Command::Republish).unwrap();
        rig.device.step().await.unwrap();
        assert_eq!(rig.published().of(inverter!("/$state")), ["init", "disconnected"]);
        drop(rig.commands);
        assert!(matches!(rig.device.step().await, Err(Error::CommandsGone)));
    }

    #[tokio::test]
    async fn another_inverter_makes_another_device() {
        let mut rig = Rig::ready();
        rig.republish().await;
        rig.requests();

        const NEW: &str = "homie/5/sofarhyd-z-9";
        rig.set_serial("Z 9");
        rig.update().await;
        let msgs = rig.published();
        let new_init = msgs.position(&format!("{NEW}/$state"), "init");
        let bridge_init = msgs.position(bridge!("/$state"), "init");
        let bridge_ready = msgs.position(bridge!("/$state"), "ready");
        let new_ready = msgs.position(&format!("{NEW}/$state"), "ready");
        assert!(new_init < bridge_init && bridge_init < bridge_ready && bridge_ready < new_ready);
        assert_eq!(msgs.of(bridge!("/$description")).len(), 1);
        assert_eq!(msgs.description(bridge!(""))["children"], json!(["sofarhyd-z-9"]));
        assert_eq!(msgs.of(&format!("{NEW}/battery/state-of-charge")), ["67"]);
        assert_eq!(msgs.description(NEW)["root"], "sofarhyd");
        assert!(msgs.deleted().is_empty());
        assert!(msgs.of(inverter!("/$state")).is_empty());
        assert_eq!(rig.inverter().protocol.id().as_str(), "sofarhyd-z-9");

        rig.set_serial("SP1ES120N12345");
        rig.update().await;
        let msgs = rig.published();
        assert_eq!(msgs.description(bridge!(""))["children"], json!(["sofarhyd-sp1es120n12345"]));
        assert_eq!(msgs.of(inverter!("/$state")), ["init", "ready"]);
        assert!(msgs.deleted().is_empty());
    }

    /// An inverter behind a fake gateway that takes writes as the real one does, with the
    /// registers of the fixture, and a device that is set up for it.
    struct Live {
        rig: Rig,
        bank: Bank,
        seen: Arc<Mutex<Vec<Req>>>,
    }

    impl Live {
        async fn start(extra: &[&str]) -> Live {
            let bank = Bank::default();
            let sample = fixture();
            for block in cli(&[]).homie.poll_plan() {
                for address in block.start..block.start + block.count {
                    sample.get(address).into_iter().for_each(|word| bank.set(address, word));
                }
            }
            // The wait time that is written together with the mode of the EPS output.
            bank.set(0x102A, 7);
            let seen = Arc::<Mutex<Vec<Req>>>::default();
            let (record, respond) = (Arc::clone(&seen), bank.responder());
            let (address, _server) = fake::start(move |request| {
                record.lock().unwrap().push(request.clone());
                respond(request)
            })
            .await;
            let address = address.to_string();
            let argv = ["--tcp", &address, "--timeout", "1s", "--request-gap", "1ms"];
            let argv: Vec<_> =
                argv.into_iter().chain(["-r", "50ms"]).chain(extra.iter().copied()).collect();
            let cli = Cli::parse_from(["test"].into_iter().chain(argv));
            let modbus::Device { handle, .. } =
                modbus::connect(&cli.modbus, cli.homie.poll_plan()).await.unwrap();
            let complete = |handle: &modbus::Handle| {
                let snapshot = handle.snapshot();
                let settings = [(0x1029, 2), (0x1104, 13), (0x1184, 38)];
                inverter::is_complete(&snapshot)
                    && settings.iter().all(|&(start, count)| snapshot.words(start, count).is_some())
            };
            let complete = async {
                while !complete(&handle) {
                    handle.changed().await;
                }
            };
            tokio::time::timeout(Duration::from_secs(5), complete).await.expect("polled");
            Live { rig: Rig::with_handle(handle), bank, seen }
        }

        fn writes(&self) -> Vec<(u16, Vec<u16>)> {
            let seen = self.seen.lock().unwrap();
            seen.iter().filter(|r| r.is_write()).map(|r| (r.start, r.written.clone())).collect()
        }

        /// Set the property (as `node/property`) and see it through.
        async fn set(&mut self, property: &str, value: &str) -> Msgs {
            let topic = format!(concat!(inverter!("/{}"), "/set"), property);
            self.rig.run(set_command(&topic, value)).await;
            self.rig.published()
        }
    }

    #[tokio::test]
    async fn a_set_is_written_and_what_the_inverter_has_is_published() {
        let mut live = Live::start(&["--allow-writes"]).await;
        live.rig.republish().await;
        let requests = live.rig.requests();
        let msgs = publishes(&requests);

        let expected: Vec<String> = [
            "inverter/enabled",
            "inverter/storage-mode",
            "inverter/clock",
            "inverter/sync-clock",
            "eps/mode",
            "passive/grid-power",
            "passive/battery-min-power",
            "passive/battery-max-power",
            "passive/timeout",
            "passive/timeout-action",
            "passive/mode",
            "passive/export-limit",
            "passive/import-limit",
        ]
        .map(|property| format!("{}/{property}/set", inverter!("")))
        .into();
        let mut subscribed = subscriptions(&requests);
        let mut expected: Vec<String> = expected;
        subscribed.sort_unstable();
        expected.sort_unstable();
        assert_eq!(subscribed, expected);
        let subscribed = requests.iter().position(|r| matches!(r, Request::Subscribe(_)));
        let ready = requests.iter().position(|r| {
            matches!(r, Request::Publish(p) if p.topic == inverter!("/$state") && p.payload == "ready")
        });
        assert!(subscribed < ready);
        let description = msgs.description(inverter!(""));
        assert_eq!(description["nodes"]["inverter"]["properties"]["enabled"]["settable"], true);
        assert_eq!(msgs.value(inverter!("/inverter/storage-mode")), "passive");

        let msgs = live.set("inverter/storage-mode", "self-use").await;
        assert_eq!(live.writes(), [(0x1110, vec![0])]);
        assert_eq!(live.bank.get(0x1110), Some(0));
        assert_eq!(msgs.value(inverter!("/inverter/storage-mode")), "self-use");

        // Unchanged, and published all the same.
        let msgs = live.set("inverter/storage-mode", "self-use").await;
        assert_eq!(msgs.of(inverter!("/inverter/storage-mode")), ["self-use"]);
    }

    #[tokio::test]
    async fn settings_across_registers_are_written_with_one_request() {
        let mut live = Live::start(&["--allow-writes"]).await;
        live.rig.republish().await;
        live.rig.requests();

        let msgs = live.set("eps/mode", "disabled").await;
        assert_eq!(live.writes(), [(0x1029, vec![0, 7])]);
        assert_eq!(msgs.value(inverter!("/eps/mode")), "disabled");

        let msgs = live.set("passive/grid-power", "-2500").await;
        assert_eq!(live.writes()[1..], [(0x1187, vec![0xFFFF, 0xF63C])]);
        assert_eq!(live.bank.get(0x1188), Some(0xF63C));
        assert_eq!(msgs.value(inverter!("/passive/grid-power")), "-2500");
    }

    #[tokio::test]
    async fn a_set_that_makes_no_sense_writes_nothing_and_the_current_value_is_published() {
        let mut live = Live::start(&["--allow-writes"]).await;
        live.rig.republish().await;
        live.rig.requests();

        let msgs = live.set("inverter/storage-mode", "turbo").await;
        assert!(live.writes().is_empty());
        assert_eq!(msgs.pairs(), [(inverter!("/inverter/storage-mode"), "passive")]);

        let msgs = live.set("passive/timeout", "70000").await;
        assert!(live.writes().is_empty());
        assert_eq!(msgs.pairs(), [(inverter!("/passive/timeout"), "600")]);
    }

    #[tokio::test]
    async fn sets_that_are_not_possible_are_ignored() {
        let mut live = Live::start(&["--allow-writes"]).await;
        live.rig.republish().await;
        live.rig.requests();

        for property in ["inverter/state", "inverter/nothing", "nothing/enabled", "pv-1/power"] {
            assert!(live.set(property, "1").await.0.is_empty(), "{property}");
        }
        let other = "homie/5/sofarhyd-other/inverter/enabled/set";
        live.rig.run(set_command(other, "false")).await;
        assert!(live.rig.published().0.is_empty());

        // Handled directly, as polling would make the inverter ready again behind our back.
        live.rig.device.inverter.as_mut().unwrap().state = Some(HomieDeviceStatus::Disconnected);
        let topic = concat!(inverter!("/inverter/enabled"), "/set");
        live.rig.device.handle_command(set_command(topic, "false")).await.unwrap();
        assert!(live.rig.device.writes.is_empty() && live.rig.published().0.is_empty());

        let mut live = Live::start(&[]).await;
        live.rig.republish().await;
        let requests = live.rig.requests();
        assert!(subscriptions(&requests).is_empty());
        let msgs = publishes(&requests);
        let description = msgs.description(inverter!(""));
        assert_ne!(description["nodes"]["inverter"]["properties"]["enabled"]["settable"], true);
        assert!(live.set("inverter/enabled", "false").await.0.is_empty());
        assert!(live.writes().is_empty());
    }

    #[tokio::test]
    async fn the_clock_is_synced_when_triggered() {
        let mut live = Live::start(&["--allow-writes"]).await;
        live.rig.republish().await;
        let description = live.rig.published().description(inverter!(""));
        let trigger = &description["nodes"]["inverter"]["properties"]["sync-clock"];
        assert_eq!((&trigger["retained"], &trigger["settable"]), (&json!(false), &json!(true)));

        assert!(live.set("inverter/sync-clock", "later").await.0.is_empty());
        assert!(live.writes().is_empty());
        let msgs = live.set("inverter/sync-clock", "now").await;
        assert_eq!(msgs.pairs(), [(inverter!("/inverter/sync-clock"), "now")]);
        let year = u16::try_from(jiff::Zoned::now().year() - 2000).unwrap();
        let [(start, words)] = &live.writes()[..] else { panic!("{:?}", live.writes()) };
        assert_eq!((*start, words.len(), words[0], words[6]), (0x1004, 7, year, 1));
    }

    #[tokio::test]
    async fn there_is_no_clock_trigger_without_writes() {
        let mut rig = Rig::ready();
        rig.republish().await;
        let description = rig.published().description(inverter!(""));
        assert!(description["nodes"]["inverter"]["properties"].get("sync-clock").is_none());
    }

    #[tokio::test]
    async fn the_inverter_is_described_anew_when_its_inputs_change() {
        let mut rig = Rig::ready();
        rig.republish().await;
        let nodes = |msgs: &Msgs| keys(&msgs.description(inverter!(""))["nodes"]).join(",") + ",";
        assert!(nodes(&rig.published()).contains("pv-2,"));
        rig.update().await;
        assert!(rig.requests().is_empty());

        // The second PV input now in parallel with the first, and another battery.
        rig.device.modbus.snapshot_mut().set_many(0x1011, &[1, 1, 128, 129]);
        rig.update().await;
        let msgs = rig.published();
        let described = nodes(&msgs);
        assert!(!described.contains("pv-2") && described.contains("battery-2,"), "{described}");
        assert_eq!(msgs.of(inverter!("/$state")), ["init", "ready"]);
        assert!(
            msgs.position(inverter!("/$state"), "init")
                < msgs.position(inverter!("/$state"), "ready")
        );

        // Nonsense is not described.
        rig.device.modbus.snapshot_mut().set_many(0x1011, &[1, 2, 3, 4, 5]);
        rig.update().await;
        assert!(rig.published().0.is_empty());
    }
}
