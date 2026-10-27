//! The connection is dropped after any request without a proper response, as there is no
//! telling what state it is in. A first timeout is the exception: the transaction ID tells a late
//! response apart.

use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;
use tokio::time::Instant;
use tracing::debug;

/// The inverter's limit is undocumented; this is what is known to work.
pub const MAX_READ: u16 = 64;

pub const MAX_WRITE: u16 = 123;

/// One timeout says little: the inverter may have been busy, and the response turns up later.
const DROP_AFTER_TIMEOUTS: u32 = 2;

const READ_HOLDING: u8 = 3;
const WRITE_MULTIPLE: u8 = 16;

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ExceptionCode(pub u8);

impl ExceptionCode {
    pub const ILLEGAL_DATA_ADDRESS: ExceptionCode = ExceptionCode(2);
}

impl std::fmt::Debug for ExceptionCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self.0 {
            1 => "IllegalFunction",
            2 => "IllegalDataAddress",
            3 => "IllegalDataValue",
            4 => "ServerDeviceFailure",
            5 => "Acknowledge",
            6 => "ServerDeviceBusy",
            8 => "MemoryParityError",
            10 => "GatewayPathUnavailable",
            11 => "GatewayTargetDeviceFailedToRespond",
            code => return write!(f, "Unknown({code})"),
        })
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("communication with the inverter failed")]
    Transport(#[source] std::io::Error),
    #[error("the inverter responded with an exception ({0:?})")]
    Exception(ExceptionCode),
    /// Nothing was sent.
    #[error("{count} registers at {start:#06x} cannot be {action} in one request")]
    InvalidRange { action: &'static str, start: u16, count: u16 },
    /// Nothing was sent.
    #[error("writing to the inverter has not been allowed")]
    WritesNotAllowed,
}

fn invalid(message: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.into())
}

fn words(data: &[u8]) -> Vec<u16> {
    data.chunks_exact(2).map(|w| u16::from_be_bytes([w[0], w[1]])).collect()
}

type Reply = Result<Vec<u16>, ExceptionCode>;

enum Op<'a> {
    Read { start: u16, count: u16 },
    Write { start: u16, words: &'a [u16] },
}

impl Op<'_> {
    const fn function(&self) -> u8 {
        match self {
            Op::Read { .. } => READ_HOLDING,
            Op::Write { .. } => WRITE_MULTIPLE,
        }
    }

    fn pdu(&self) -> Vec<u8> {
        let mut pdu = vec![self.function()];
        match *self {
            Op::Read { start, count } => {
                pdu.extend(start.to_be_bytes());
                pdu.extend(count.to_be_bytes());
            }
            Op::Write { start, words } => {
                pdu.extend(start.to_be_bytes());
                pdu.extend((words.len() as u16).to_be_bytes());
                pdu.push(2 * words.len() as u8);
                pdu.extend(words.iter().flat_map(|w| w.to_be_bytes()));
            }
        }
        pdu
    }

    fn parse(&self, pdu: &[u8]) -> std::io::Result<Reply> {
        let function = self.function();
        match (self, pdu) {
            (_, [f, code]) if *f == function | 0x80 => Ok(Err(ExceptionCode(*code))),
            (Op::Read { count, .. }, [f, bytes, data @ ..])
                if *f == function
                    && usize::from(*bytes) == 2 * usize::from(*count)
                    && data.len() == usize::from(*bytes) =>
            {
                Ok(Ok(words(data)))
            }
            (Op::Write { start, words }, [f, a, b, c, d])
                if *f == function
                    && u16::from_be_bytes([*a, *b]) == *start
                    && usize::from(u16::from_be_bytes([*c, *d])) == words.len() =>
            {
                Ok(Ok(Vec::new()))
            }
            _ => Err(invalid(format!("unexpected response PDU {pdu:02x?}"))),
        }
    }
}

struct Connection {
    stream: TcpStream,
    /// So that giving up on a response does not lose a part of one.
    buffer: Vec<u8>,
}

impl Connection {
    fn check_idle(&mut self) -> std::io::Result<()> {
        let mut chunk = [0u8; 256];
        for _ in 0..64 {
            match self.stream.try_read(&mut chunk) {
                Ok(0) => return Err(std::io::ErrorKind::UnexpectedEof.into()),
                Ok(n) => {
                    debug!(bytes = n, "received data nobody has asked for");
                    self.buffer.extend(&chunk[..n]);
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(()),
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Cancellation safe.
    async fn frame(&mut self, unit_id: u8) -> std::io::Result<(u16, Vec<u8>)> {
        loop {
            if let Some(header) = self.buffer.first_chunk::<7>() {
                let transaction = u16::from_be_bytes([header[0], header[1]]);
                let protocol = u16::from_be_bytes([header[2], header[3]]);
                let length = usize::from(u16::from_be_bytes([header[4], header[5]]));
                if protocol != 0 || !(3..=254).contains(&length) {
                    return Err(invalid(format!("unexpected frame header {header:02x?}")));
                }
                if header[6] != unit_id {
                    return Err(invalid(format!(
                        "response from unit {}, not {unit_id}",
                        header[6]
                    )));
                }
                let total = 6 + length;
                if self.buffer.len() >= total {
                    let pdu = self.buffer[7..total].to_vec();
                    self.buffer.drain(..total);
                    return Ok((transaction, pdu));
                }
            }
            let mut chunk = [0u8; 256];
            match self.stream.read(&mut chunk).await? {
                0 => return Err(std::io::ErrorKind::UnexpectedEof.into()),
                n => self.buffer.extend(&chunk[..n]),
            }
        }
    }

    /// Frames with other IDs answer requests that were given up on. Cancellation safe.
    async fn receive(
        &mut self,
        transaction: u16,
        unit_id: u8,
        op: &Op<'_>,
    ) -> std::io::Result<Reply> {
        loop {
            let (received, pdu) = self.frame(unit_id).await?;
            if received == transaction {
                return op.parse(&pdu);
            }
            debug!(transaction = received, expected = transaction, "discarded a stale response");
        }
    }
}

pub(crate) async fn resolve(tcp: &str) -> std::io::Result<SocketAddr> {
    if let Ok(address) = tcp.parse::<SocketAddr>() {
        return Ok(address);
    }
    let unbracketed = tcp.strip_prefix('[').and_then(|ip| ip.strip_suffix(']')).unwrap_or(tcp);
    if let Ok(ip) = unbracketed.parse::<std::net::IpAddr>() {
        return Ok(SocketAddr::new(ip, 502));
    }
    let (host, port) = tcp
        .rsplit_once(':')
        .and_then(|(host, port)| Some((host, port.parse::<u16>().ok()?)))
        .unwrap_or((tcp, 502));
    tokio::net::lookup_host((host, port)).await?.next().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::NotFound, "did not resolve to any addresses")
    })
}

pub struct Client {
    /// Resolved anew for every connection, in case the gateway moves.
    tcp: String,
    unit_id: u8,
    timeout: Duration,
    request_gap: Duration,
    writes_allowed: bool,
    connection: Option<Connection>,
    next_transaction: u16,
    timeouts: u32,
    last_exchange: Option<Instant>,
}

impl Client {
    pub fn new(tcp: String, unit_id: u8, timeout: Duration, request_gap: Duration) -> Client {
        Client {
            tcp,
            unit_id,
            timeout,
            request_gap,
            writes_allowed: false,
            connection: None,
            next_transaction: 0,
            timeouts: 0,
            last_exchange: None,
        }
    }

    pub fn allow_writes(mut self, allowed: bool) -> Client {
        self.writes_allowed = allowed;
        self
    }

    /// Out of `self` for the duration of a request, so that one abandoned half way cannot leave
    /// the connection to be used again.
    async fn connect(&mut self) -> std::io::Result<Connection> {
        if let Some(mut connection) = self.connection.take() {
            match connection.check_idle() {
                Ok(()) => return Ok(connection),
                Err(e) => debug!(error = %e, "the connection was closed while idle"),
            }
        }
        let opening = async {
            let stream = TcpStream::connect(resolve(&self.tcp).await?).await?;
            stream.set_nodelay(true)?;
            Ok::<_, std::io::Error>(stream)
        };
        let stream = tokio::time::timeout(self.timeout, opening)
            .await
            .unwrap_or_else(|_| Err(std::io::ErrorKind::TimedOut.into()))?;
        debug!("connected to the inverter");
        Ok(Connection { stream, buffer: Vec::new() })
    }

    /// Takes `&mut self` as the bus deals with one request at a time.
    pub async fn read(&mut self, start: u16, count: u16) -> Result<Vec<u16>, Error> {
        check_range("read", start, count, MAX_READ)?;
        self.request(Op::Read { start, count }).await
    }

    pub async fn write(&mut self, start: u16, words: &[u16]) -> Result<(), Error> {
        if !self.writes_allowed {
            return Err(Error::WritesNotAllowed);
        }
        let count = u16::try_from(words.len()).unwrap_or(u16::MAX);
        check_range("written", start, count, MAX_WRITE)?;
        self.request(Op::Write { start, words }).await.map(drop)
    }

    async fn request(&mut self, op: Op<'_>) -> Result<Vec<u16>, Error> {
        if let Some(last) = self.last_exchange {
            tokio::time::sleep_until(last + self.request_gap).await;
        }
        let result = self.exchange(&op).await;
        self.last_exchange = Some(Instant::now());
        result
    }

    async fn exchange(&mut self, op: &Op<'_>) -> Result<Vec<u16>, Error> {
        let mut connection = self.connect().await.map_err(|e| self.lost(e))?;
        let started = Instant::now();
        let deadline = started + self.timeout;
        let transaction = self.next_transaction;
        self.next_transaction = transaction.wrapping_add(1);

        let pdu = op.pdu();
        let mut frame = Vec::with_capacity(7 + pdu.len());
        frame.extend(transaction.to_be_bytes());
        frame.extend([0, 0]);
        frame.extend((pdu.len() as u16 + 1).to_be_bytes());
        frame.push(self.unit_id);
        frame.extend(pdu);
        // A request that is only partially sent leaves the connection of no use.
        match tokio::time::timeout_at(deadline, connection.stream.write_all(&frame)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(self.lost(e)),
            Err(_) => return Err(self.lost(std::io::ErrorKind::TimedOut.into())),
        }

        let received = connection.receive(transaction, self.unit_id, op);
        match tokio::time::timeout_at(deadline, received).await {
            Ok(Ok(reply)) => {
                self.timeouts = 0;
                self.connection = Some(connection);
                debug!(elapsed = ?started.elapsed(), "request completed");
                reply.map_err(Error::Exception)
            }
            Ok(Err(e)) => Err(self.lost(e)),
            Err(_) => {
                self.timeouts += 1;
                if self.timeouts < DROP_AFTER_TIMEOUTS {
                    // The response may well turn up later, and will be recognized as stale.
                    self.connection = Some(connection);
                    return Err(Error::Transport(std::io::ErrorKind::TimedOut.into()));
                }
                Err(self.lost(std::io::ErrorKind::TimedOut.into()))
            }
        }
    }

    fn lost(&mut self, error: std::io::Error) -> Error {
        self.timeouts = 0;
        debug!(%error, "the connection to the inverter is not in use anymore");
        Error::Transport(error)
    }
}

fn check_range(action: &'static str, start: u16, count: u16, max: u16) -> Result<(), Error> {
    if !(1..=max).contains(&count) || u32::from(start) + u32::from(count) > 0x1_0000 {
        return Err(Error::InvalidRange { action, start, count });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modbus::fake::{self, Bank, Step};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

    fn client(address: SocketAddr) -> Client {
        let (timeout, gap) = (Duration::from_millis(150), Duration::from_millis(1));
        Client::new(address.to_string(), 1, timeout, gap).allow_writes(true)
    }

    fn is_transport<T: std::fmt::Debug>(result: Result<T, Error>) -> bool {
        matches!(result, Err(Error::Transport(_)))
    }

    fn is_invalid_data<T: std::fmt::Debug>(result: Result<T, Error>) -> bool {
        matches!(&result, Err(Error::Transport(e)) if e.kind() == std::io::ErrorKind::InvalidData)
    }

    fn with_length(mut frame: Vec<u8>, length: u16) -> Vec<u8> {
        frame[4..6].copy_from_slice(&length.to_be_bytes());
        frame
    }

    #[tokio::test]
    async fn round_trips() {
        let bank = Bank::default();
        let (address, _) = fake::start(bank.responder()).await;
        let mut client = client(address);
        assert_eq!(client.read(10, 3).await.unwrap(), [3010, 3011, 3012]);
        assert_eq!(client.read(0xFFFF, 1).await.unwrap(), [2999]);
        assert_eq!(client.read(1, MAX_READ).await.unwrap().len(), usize::from(MAX_READ));
        client.write(0x1029, &[1, 2]).await.unwrap();
        assert_eq!((bank.get(0x1029), bank.get(0x102A)), (Some(1), Some(2)));
        assert_eq!(client.read(0x1028, 4).await.unwrap(), [7136, 1, 2, 7139]);
        let all = vec![7; usize::from(MAX_WRITE)];
        client.write(0xFFFF - MAX_WRITE + 1, &all).await.unwrap();
        assert_eq!(bank.get(0xFFFF), Some(7));
    }

    #[tokio::test]
    async fn exceptions_are_reported_and_cost_nothing() {
        let accepted = Arc::new(AtomicUsize::new(0));
        let (address, _) = fake::start_counting(accepted.clone(), |request| match request.n {
            0 | 2 => vec![Step::Send(request.exception(2))],
            _ => fake::ok(request),
        })
        .await;
        let mut client = client(address);
        let failed = client.read(1, 1).await;
        assert!(matches!(failed, Err(Error::Exception(ExceptionCode::ILLEGAL_DATA_ADDRESS))));
        assert_eq!(client.read(1, 1).await.unwrap(), [3001]);
        let failed = client.write(1, &[1]).await;
        assert!(matches!(failed, Err(Error::Exception(ExceptionCode::ILLEGAL_DATA_ADDRESS))));
        client.write(1, &[1]).await.unwrap();
        assert_eq!(accepted.load(SeqCst), 1);
    }

    #[tokio::test]
    async fn nonsense_responses_are_rejected_and_cost_the_connection() {
        let accepted = Arc::new(AtomicUsize::new(0));
        let (address, _) = fake::start_counting(accepted.clone(), |request| {
            let frame = match request.n {
                0 => fake::mbap(request.transaction, 1, 1, &[3, 2, 0, 7]), // another protocol
                1 => with_length(request.data_frame(), 0),
                2 => with_length(request.data_frame(), 255),
                3 => request.frame_from(2, &[3, 2, 0, 7]), // another unit
                4 => request.frame_from(1, &[4, 2, 0, 7]), // the wrong function code
                5 => request.frame_from(1, &[3, 4, 0, 7, 0, 8]), // too many bytes
                6 => request.frame_from(1, &[3, 1, 7]),    // an odd number of bytes
                7 => request.frame_from(1, &[3, 2, 0, 7, 0]), // trailing bytes
                _ => return fake::ok(request),
            };
            vec![Step::Send(frame)]
        })
        .await;
        let mut client = client(address);
        for _ in 0..8 {
            assert!(is_invalid_data(client.read(1, 1).await));
        }
        assert_eq!(client.read(1, 1).await.unwrap(), [3001]);
        assert_eq!(accepted.load(SeqCst), 9);
    }

    #[tokio::test]
    async fn nonsense_responses_to_writes_are_rejected() {
        let accepted = Arc::new(AtomicUsize::new(0));
        let (address, _) = fake::start_counting(accepted.clone(), |request| {
            let frame = match request.n {
                0 => request.echo_frame_with(request.start + 1, request.count),
                1 => request.echo_frame_with(request.start, request.count + 1),
                2 => request.data_frame_with([request.start]),
                3 => request.frame_from(1, &[16, 0, 1, 0]), // cut short
                _ => return fake::ok(request),
            };
            vec![Step::Send(frame)]
        })
        .await;
        let mut client = client(address);
        for _ in 0..4 {
            assert!(is_invalid_data(client.write(1, &[5]).await));
        }
        client.write(1, &[5]).await.unwrap();
        assert_eq!(accepted.load(SeqCst), 5);
    }

    #[tokio::test]
    async fn a_response_in_pieces_is_put_together() {
        let (address, _) = fake::start(|request| {
            let frame = request.data_frame();
            let wait = Step::Wait(Duration::from_millis(20));
            vec![Step::Send(frame[..3].to_vec()), wait, Step::Send(frame[3..].to_vec())]
        })
        .await;
        let mut client = client(address);
        assert_eq!(client.read(5, 2).await.unwrap(), [3005, 3006]);
    }

    #[tokio::test]
    async fn stale_responses_are_skipped() {
        let accepted = Arc::new(AtomicUsize::new(0));
        let (address, _) = fake::start_counting(accepted.clone(), |request| match request.n {
            // Only gets to respond after having been given up on.
            0 => vec![Step::Wait(Duration::from_millis(250)), Step::Send(request.data_frame())],
            // Both of these turn up before the response, with the right size and all.
            1 => {
                let stale = |t| request.with_transaction(t).data_frame_with([9999]);
                let stale = [Step::Send(stale(0)), Step::Send(stale(40000))];
                stale.into_iter().chain(fake::ok(request)).collect()
            }
            _ => fake::ok(request),
        })
        .await;
        let mut client = client(address);
        let timed_out = client.read(5, 1).await;
        assert!(
            matches!(&timed_out, Err(Error::Transport(e)) if e.kind() == std::io::ErrorKind::TimedOut)
        );
        // The response to the first request is to another register, so mistaking it for the
        // response to this one would show.
        assert_eq!(client.read(6, 1).await.unwrap(), [3006]);
        assert_eq!(client.read(7, 1).await.unwrap(), [3007]);
        assert_eq!(accepted.load(SeqCst), 1);
    }

    #[tokio::test]
    async fn a_connection_is_kept_after_a_timeout_but_not_after_two_in_a_row() {
        let accepted = Arc::new(AtomicUsize::new(0));
        // Not responded to: the 1st, the 3rd and the 4th.
        let (address, _) = fake::start_counting(accepted.clone(), |request| match request.n {
            0 | 2 | 3 => vec![],
            _ => fake::ok(request),
        })
        .await;
        let mut client = client(address);
        assert!(is_transport(client.read(1, 1).await));
        assert_eq!(client.read(1, 1).await.unwrap(), [3001]);
        assert_eq!(accepted.load(SeqCst), 1);
        assert!(is_transport(client.read(1, 1).await));
        assert!(is_transport(client.read(1, 1).await));
        // Two in a row: a new connection from now on.
        assert_eq!(client.read(1, 1).await.unwrap(), [3001]);
        assert_eq!(accepted.load(SeqCst), 2);
        assert_eq!(client.read(1, 1).await.unwrap(), [3001]);
        assert_eq!(accepted.load(SeqCst), 2);
    }

    #[tokio::test]
    async fn a_connection_that_is_closed_is_opened_again() {
        let accepted = Arc::new(AtomicUsize::new(0));
        let (address, _) = fake::start_counting(accepted.clone(), |request| match request.n {
            0 => vec![Step::Close],
            1 => [Step::Send(request.data_frame()), Step::Close].into(),
            _ => fake::ok(request),
        })
        .await;
        let mut client = client(address);
        assert!(is_transport(client.read(1, 1).await));
        assert_eq!(client.read(1, 1).await.unwrap(), [3001]);
        // Noticed before the request is sent, so there is nothing to fail.
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(client.read(1, 1).await.unwrap(), [3001]);
        assert_eq!(accepted.load(SeqCst), 3);
    }

    #[tokio::test]
    async fn requests_are_spaced_out_and_bad_ones_not_sent() {
        let arrivals = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = arrivals.clone();
        let (address, _) = fake::start(move |request| {
            seen.lock().unwrap().push(Instant::now());
            fake::ok(request)
        })
        .await;
        let mut client = client(address);
        client.request_gap = Duration::from_millis(100);
        client.read(1, 1).await.unwrap();
        client.write(1, &[1]).await.unwrap();
        client.read(1, 1).await.unwrap();
        let arrivals = arrivals.lock().unwrap().clone();
        assert!(arrivals.windows(2).all(|w| w[1] - w[0] >= Duration::from_millis(95)));

        for (start, count) in [(0, 0), (0, MAX_READ + 1), (65535, 2), (0xFFC1, 0x40)] {
            let failed = client.read(start, count).await;
            assert!(matches!(failed, Err(Error::InvalidRange { .. })), "{failed:?}");
        }
        let too_long = vec![0; usize::from(MAX_WRITE) + 1];
        for (start, words) in [(0, &[][..]), (0, &too_long[..]), (65535, &[1, 2][..])] {
            let failed = client.write(start, words).await;
            assert!(matches!(failed, Err(Error::InvalidRange { .. })), "{failed:?}");
        }
        assert_eq!(arrivals.len(), 3);
    }

    #[tokio::test]
    async fn writes_are_refused_unless_allowed() {
        let arrivals = Arc::new(AtomicUsize::new(0));
        let seen = arrivals.clone();
        let (address, _) = fake::start(move |request| {
            seen.fetch_add(1, SeqCst);
            fake::ok(request)
        })
        .await;
        let mut client =
            Client::new(address.to_string(), 1, Duration::from_millis(150), Duration::ZERO);
        let failed = client.write(1, &[1]).await;
        assert!(matches!(failed, Err(Error::WritesNotAllowed)), "{failed:?}");
        client.read(1, 1).await.unwrap();
        assert_eq!(arrivals.load(SeqCst), 1);
        let mut client = client.allow_writes(true);
        client.write(1, &[1]).await.unwrap();
        assert_eq!(arrivals.load(SeqCst), 2);
        assert!(matches!(
            client.allow_writes(false).write(1, &[1]).await,
            Err(Error::WritesNotAllowed)
        ));
        assert_eq!(arrivals.load(SeqCst), 2);
    }

    #[tokio::test]
    async fn nothing_listening_is_an_error() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = client(listener.local_addr().unwrap());
        drop(listener);
        assert!(is_transport(client.read(1, 1).await));
    }

    #[tokio::test]
    async fn addresses_are_resolved() {
        assert_eq!(resolve("127.0.0.1").await.unwrap(), "127.0.0.1:502".parse().unwrap());
        assert_eq!(resolve("127.0.0.1:1502").await.unwrap(), "127.0.0.1:1502".parse().unwrap());
        assert_eq!(resolve("[::1]").await.unwrap(), "[::1]:502".parse().unwrap());
        assert_eq!(resolve("[::1]:7").await.unwrap(), "[::1]:7".parse().unwrap());
    }
}
