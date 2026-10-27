mod client;
#[cfg(test)]
pub(crate) mod fake;
mod snapshot;

pub use client::{Client, Error as RequestError, ExceptionCode, MAX_READ, MAX_WRITE};
pub use snapshot::Snapshot;

use std::sync::{Arc, RwLock, RwLockReadGuard};
use std::time::Duration;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::time::Instant;
use tracing::{debug, info, warn};

/// Registers may appear once an option is installed.
const RETRY_MISSING_EVERY: Duration = Duration::from_secs(10 * 60);

#[derive(Debug, Clone, Copy)]
struct Backoff {
    base: Duration,
    max: Duration,
}

impl Backoff {
    const DEFAULT: Backoff = Backoff { base: Duration::from_secs(1), max: Duration::from_secs(10) };

    fn delay(&self, failures: u32) -> Duration {
        match failures {
            0 | 1 => Duration::ZERO,
            n => self.base.saturating_mul(1 << (n - 2).min(16)).min(self.max),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("lookup of `{1}` failed")]
    LookupHost(#[source] std::io::Error, String),
    #[error("polling {0} registers at {1} would go past the end of the address space")]
    BlockOutOfRange(u16, u16),
}

#[derive(thiserror::Error, Debug)]
pub enum WriteError {
    #[error("writing to the inverter has not been allowed")]
    NotAllowed,
    /// A failed read-back does not mean the write was not done.
    #[error("the request to the inverter failed")]
    Request(#[source] RequestError),
    #[error("the worker that talks to the inverter has stopped")]
    Stopped,
}

#[derive(clap::Parser, Clone)]
#[group(id = "modbus::Args")]
pub struct Args {
    /// The address of the Modbus TCP gateway, as `host` or `host:port` (502 by default).
    #[arg(long, short = 't')]
    tcp: String,

    /// The Modbus unit ID (address) of the inverter.
    #[arg(long, short = 'i', default_value_t = 1)]
    unit_id: u8,

    /// How long to wait for a response.
    #[arg(long, default_value = "3s")]
    timeout: humantime::Duration,

    /// How long to leave the bus alone between consecutive requests.
    #[arg(long, default_value = "20ms")]
    request_gap: humantime::Duration,

    /// How long the inverter has to go without a single successful read to be considered
    /// unavailable.
    #[arg(long, default_value = "60s")]
    unavailable_after: humantime::Duration,

    /// Allow writing to the inverter. Without it nothing is ever written.
    #[arg(long)]
    allow_writes: bool,
}

impl Args {
    pub fn allow_writes(&self) -> bool {
        self.allow_writes
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PollBlock {
    pub start: u16,
    pub count: u16,
    pub period: Duration,
}

struct Shared {
    snapshot: RwLock<Snapshot>,
    changed: Notify,
    unavailable_after: Duration,
}

impl Shared {
    fn read(&self) -> RwLockReadGuard<'_, Snapshot> {
        self.snapshot.read().unwrap_or_else(|e| e.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Snapshot> {
        self.snapshot.write().unwrap_or_else(|e| e.into_inner())
    }

    fn expires_at(&self) -> Option<Instant> {
        self.read().expires_at(self.unavailable_after)
    }

    fn expire(&self) {
        let mut snapshot = self.write();
        if snapshot.expires_at(self.unavailable_after).is_some_and(|at| at <= Instant::now()) {
            snapshot.set_available(false);
            drop(snapshot);
            warn!(after = ?self.unavailable_after, "no reads have succeeded for a while, the inverter is unavailable");
            self.changed.notify_one();
        }
    }
}

struct WriteCommand {
    start: u16,
    words: Vec<u16>,
    reply: oneshot::Sender<Result<Vec<u16>, WriteError>>,
}

#[derive(Clone)]
pub struct Handle {
    shared: Arc<Shared>,
    /// Closing it stops the worker.
    commands: mpsc::Sender<WriteCommand>,
    writes_allowed: bool,
}

impl Handle {
    #[cfg(test)]
    pub(crate) fn detached(snapshot: Snapshot) -> Handle {
        let shared = Shared {
            snapshot: RwLock::new(snapshot),
            changed: Notify::new(),
            unavailable_after: Duration::ZERO,
        };
        Handle { shared: Arc::new(shared), commands: mpsc::channel(1).0, writes_allowed: false }
    }

    /// Do not hold on to it: it blocks the updates.
    pub fn snapshot(&self) -> RwLockReadGuard<'_, Snapshot> {
        self.shared.read()
    }

    #[cfg(test)]
    #[allow(dead_code)] // for the tests of whatever makes sense of the registers
    pub(crate) fn snapshot_mut(&self) -> std::sync::RwLockWriteGuard<'_, Snapshot> {
        self.shared.write()
    }

    /// There is one consumer; changes while nobody waits are remembered as one.
    pub async fn changed(&self) {
        self.shared.changed.notified().await;
    }

    pub fn writes_allowed(&self) -> bool {
        self.writes_allowed
    }

    /// All the words go in one request, as some settings span several registers. Returns what the
    /// registers read back, as the inverter may acknowledge values without taking them. Dropping
    /// the future does not cancel a write that is already on its way.
    pub async fn write(&self, start: u16, words: Vec<u16>) -> Result<Vec<u16>, WriteError> {
        if !self.writes_allowed {
            return Err(WriteError::NotAllowed);
        }
        let (reply, response) = oneshot::channel();
        let command = WriteCommand { start, words, reply };
        self.commands.send(command).await.map_err(|_| WriteError::Stopped)?;
        response.await.map_err(|_| WriteError::Stopped)?
    }
}

pub struct Device {
    pub handle: Handle,
    /// Connection problems do not end it.
    pub task: tokio::task::JoinHandle<()>,
}

/// Resolved up front to catch mistakes early; every connection resolves it again.
async fn client(args: &Args, writes_allowed: bool) -> Result<Client, Error> {
    let address =
        client::resolve(&args.tcp).await.map_err(|e| Error::LookupHost(e, args.tcp.clone()))?;
    info!(%address, "gateway address resolved");
    let client = Client::new(args.tcp.clone(), args.unit_id, *args.timeout, *args.request_gap);
    Ok(client.allow_writes(writes_allowed))
}

pub async fn connect(args: &Args, blocks: Vec<PollBlock>) -> Result<Device, Error> {
    connect_tuned(args, blocks, Tuning::default()).await
}

/// Cannot write.
pub async fn read_once(
    args: &Args,
    blocks: &[PollBlock],
) -> Result<(Snapshot, Vec<(PollBlock, RequestError)>), Error> {
    let mut client = client(args, false).await?;
    let mut snapshot = Snapshot::new();
    let mut failed = Vec::new();
    for block in split(blocks.iter().copied())? {
        let Block { start, count, period, .. } = block;
        match client.read(start, count).await {
            Ok(words) => {
                snapshot.set_many(start, &words);
                snapshot.record_success(Instant::now());
            }
            Err(e) => {
                debug!(error = (&e as &dyn std::error::Error), %block, "could not read out the registers");
                failed.push((PollBlock { start, count, period }, e));
            }
        }
    }
    Ok((snapshot, failed))
}

struct Tuning {
    retry_missing_every: Duration,
    backoff: Backoff,
}

impl Default for Tuning {
    fn default() -> Self {
        Tuning { retry_missing_every: RETRY_MISSING_EVERY, backoff: Backoff::DEFAULT }
    }
}

async fn connect_tuned(
    args: &Args,
    blocks: Vec<PollBlock>,
    tuning: Tuning,
) -> Result<Device, Error> {
    let client = client(args, args.allow_writes).await?;
    let blocks = split(blocks)?;
    info!(blocks = blocks.len(), writes_allowed = args.allow_writes, "polling plan");
    let shared = Shared {
        snapshot: RwLock::default(),
        changed: Notify::new(),
        unavailable_after: *args.unavailable_after,
    };
    let shared = Arc::new(shared);
    let (commands, pending) = mpsc::channel(8);
    let worker = Worker {
        client,
        shared: Arc::clone(&shared),
        blocks,
        retry_missing_every: tuning.retry_missing_every,
        backoff: tuning.backoff,
    };
    let task = tokio::task::spawn(worker.run(pending));
    let handle = Handle { shared, commands, writes_allowed: args.allow_writes };
    Ok(Device { handle, task })
}

#[derive(Clone, Copy, PartialEq)]
enum Status {
    Ok,
    Missing,
    Failing(u32),
}

struct Block {
    start: u16,
    count: u16,
    period: Duration,
    next_due: Instant,
    status: Status,
}

impl std::fmt::Display for Block {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let end = u32::from(self.start) + u32::from(self.count) - 1;
        write!(f, "registers {:#06x}..={end:#06x}", self.start)
    }
}

fn split(blocks: impl IntoIterator<Item = PollBlock>) -> Result<Vec<Block>, Error> {
    let now = Instant::now();
    let mut parts = Vec::new();
    for PollBlock { start, count, period } in blocks {
        let end = u32::from(start) + u32::from(count);
        if end > 0x1_0000 {
            return Err(Error::BlockOutOfRange(count, start));
        }
        for start in (u32::from(start)..end).step_by(MAX_READ.into()) {
            let count = (end - start).min(MAX_READ.into()) as u16;
            let (start, next_due, status) = (start as u16, now, Status::Ok);
            parts.push(Block { start, count, period, next_due, status });
        }
    }
    Ok(parts)
}

async fn sleep_until_or_never(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// A request cannot be abandoned half way, but the inverter becoming unavailable is not to wait
/// for it.
async fn until_done<T>(shared: &Shared, request: impl Future<Output = T>) -> T {
    tokio::pin!(request);
    loop {
        tokio::select! {
            result = &mut request => return result,
            () = sleep_until_or_never(shared.expires_at()) => shared.expire(),
        }
    }
}

struct Worker {
    client: Client,
    shared: Arc<Shared>,
    blocks: Vec<Block>,
    retry_missing_every: Duration,
    backoff: Backoff,
}

impl Worker {
    async fn run(mut self, mut commands: mpsc::Receiver<WriteCommand>) {
        loop {
            let next = self.blocks.iter().enumerate().min_by_key(|(_, block)| block.next_due);
            let next = next.map(|(index, block)| (index, block.next_due));
            tokio::select! {
                // Writes go ahead of the polls that are due.
                biased;
                command = commands.recv() => match command {
                    Some(command) => self.write(command).await,
                    None => return,
                },
                () = sleep_until_or_never(next.map(|(_, due)| due)) => {
                    self.poll(next.expect("woken up by a block being due").0).await;
                }
                () = sleep_until_or_never(self.shared.expires_at()) => self.shared.expire(),
            }
        }
    }

    async fn poll(&mut self, index: usize) {
        let Block { start, count, .. } = self.blocks[index];
        let started = Instant::now();
        let result = until_done(&self.shared, self.client.read(start, count)).await;
        self.finish(index, started, result);
    }

    async fn write(&mut self, command: WriteCommand) {
        let WriteCommand { start, words, reply } = command;
        let result = self.write_and_read_back(start, &words).await;
        if reply.send(result).is_err() {
            debug!(start, "nobody is waiting for the outcome of the write");
        }
    }

    async fn write_and_read_back(
        &mut self,
        start: u16,
        words: &[u16],
    ) -> Result<Vec<u16>, WriteError> {
        let started = Instant::now();
        let written = until_done(&self.shared, self.client.write(start, words)).await;
        if let Err(e) = written {
            warn!(error = (&e as &dyn std::error::Error), start, "could not write the registers");
            return Err(WriteError::Request(e));
        }
        self.succeeded(Instant::now());

        let mut read_back = Vec::with_capacity(words.len());
        for offset in (0..words.len()).step_by(MAX_READ.into()) {
            let count = (words.len() - offset).min(MAX_READ.into());
            let (start, count) = (start + offset as u16, count as u16);
            let read = until_done(&self.shared, self.client.read(start, count)).await;
            match read {
                Ok(chunk) => read_back.extend(chunk),
                Err(e) => {
                    warn!(
                        error = (&e as &dyn std::error::Error),
                        start, "could not read back the registers"
                    );
                    return Err(WriteError::Request(e));
                }
            }
        }
        debug!(start, count = words.len(), took = ?started.elapsed(), "written");
        self.store(start, &read_back, Instant::now());
        // Even if nothing changed: the value is to be published again.
        self.shared.changed.notify_one();
        Ok(read_back)
    }

    fn succeeded(&self, now: Instant) {
        let available = self.shared.write().record_success(now);
        if available {
            info!("the inverter is available");
            self.shared.changed.notify_one();
        }
    }

    /// Under a single lock, so that values that span several registers are consistent.
    fn store(&self, start: u16, words: &[u16], now: Instant) {
        let mut snapshot = self.shared.write();
        let stored = snapshot.set_many(start, words);
        let available = snapshot.record_success(now);
        drop(snapshot);
        if available {
            info!("the inverter is available");
        }
        if stored || available {
            debug!(start, available, "registers changed");
            self.shared.changed.notify_one();
        }
    }

    fn finish(&mut self, index: usize, started: Instant, result: Result<Vec<u16>, RequestError>) {
        let now = Instant::now();
        let block = &mut self.blocks[index];
        block.next_due = (block.next_due + block.period).max(now);
        let was = block.status;
        match result {
            Ok(words) => {
                match was {
                    Status::Ok => {}
                    Status::Missing => {
                        info!(%block, "the inverter has something at these addresses now")
                    }
                    Status::Failing(_) => info!(%block, "the registers can be read out again"),
                }
                block.status = Status::Ok;
                debug!(%block, took = ?started.elapsed(), "read out");
                let start = block.start;
                self.store(start, &words, now);
            }
            Err(RequestError::Exception(ExceptionCode::ILLEGAL_DATA_ADDRESS)) => {
                if was != Status::Missing {
                    let retry_in = self.retry_missing_every;
                    info!(%block, ?retry_in, "the inverter has nothing at some of these addresses");
                }
                block.status = Status::Missing;
                block.next_due = now + self.retry_missing_every;
            }
            Err(e) => {
                if !matches!(was, Status::Failing(_)) {
                    warn!(error = (&e as &dyn std::error::Error), %block, "could not read out the registers");
                }
                block.status = Status::Failing(match (&e, was) {
                    // The inverter would only say the same if asked again at once.
                    (RequestError::Exception(_), _) => 0,
                    (_, Status::Failing(n)) => n.saturating_add(1),
                    _ => 1,
                });
                if let (Status::Failing(n), RequestError::Transport(_)) = (block.status, &e) {
                    // Back off rather than wait for the whole period.
                    block.next_due = now + self.backoff.delay(n).min(block.period);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser as _;
    use fake::{Bank, Req, Step};
    use std::sync::atomic::{AtomicU16, Ordering::SeqCst};
    use std::sync::{Arc, Mutex};

    #[derive(clap::Parser)]
    struct Cli {
        #[clap(flatten)]
        modbus: Args,
    }

    fn args(address: std::net::SocketAddr, unavailable_after: &str) -> Args {
        let address = address.to_string();
        let argv = ["test", "--tcp", &address, "--timeout", "300ms", "--request-gap", "1ms"];
        let argv = argv.into_iter().chain(["--unavailable-after", unavailable_after]);
        Cli::try_parse_from(argv).unwrap().modbus
    }

    fn args_allowing_writes(address: std::net::SocketAddr) -> Args {
        let address = address.to_string();
        let argv = ["test", "--tcp", &address, "--timeout", "300ms", "--request-gap", "1ms"];
        Cli::try_parse_from(argv.into_iter().chain(["--allow-writes"])).unwrap().modbus
    }

    fn tuning() -> Tuning {
        let backoff = Backoff { base: Duration::from_millis(5), max: Duration::from_millis(20) };
        Tuning { retry_missing_every: Duration::from_secs(600), backoff }
    }

    fn block(start: u16, count: u16) -> PollBlock {
        PollBlock { start, count, period: Duration::from_millis(20) }
    }

    async fn changed_within(handle: &Handle, millis: u64) -> bool {
        tokio::time::timeout(Duration::from_millis(millis), handle.changed()).await.is_ok()
    }

    type Arrivals = Arc<Mutex<Vec<(Instant, Req)>>>;

    fn recording(
        respond: impl Fn(&Req) -> Vec<Step> + Send + Sync + 'static,
    ) -> (Arrivals, impl Fn(&Req) -> Vec<Step> + Send + Sync + 'static) {
        let arrivals = Arrivals::default();
        let seen = Arc::clone(&arrivals);
        let respond = move |request: &Req| {
            seen.lock().unwrap().push((Instant::now(), request.clone()));
            respond(request)
        };
        (arrivals, respond)
    }

    fn requests(arrivals: &Arrivals) -> Vec<(u8, u16)> {
        arrivals.lock().unwrap().iter().map(|(_, r)| (r.function, r.start)).collect()
    }

    #[test]
    fn long_blocks_are_split_and_delays_back_off() {
        let blocks = split(vec![block(100, 200), block(0, 5)]);
        let ranges: Vec<_> = blocks.unwrap().iter().map(|b| (b.start, b.count)).collect();
        assert_eq!(ranges, [(100, 64), (164, 64), (228, 64), (292, 8), (0, 5)]);
        let blocks = split(vec![block(65400, 136)]).unwrap();
        assert_eq!(blocks.iter().map(|b| b.count).collect::<Vec<_>>(), [64, 64, 8]);
        assert!(split(vec![block(65400, 137)]).is_err());

        let delays: Vec<_> = (0..8).map(|n| Backoff::DEFAULT.delay(n).as_secs()).collect();
        assert_eq!(delays, [0, 0, 1, 2, 4, 8, 10, 10]);
        assert_eq!(Backoff::DEFAULT.delay(u32::MAX), Duration::from_secs(10));
    }

    #[tokio::test]
    async fn polling_fills_the_snapshot_and_only_changes_notify() {
        let offset = Arc::new(AtomicU16::new(0));
        let shift = Arc::clone(&offset);
        let (address, _server) = fake::start(move |request| {
            let shift = shift.load(SeqCst);
            vec![Step::Send(
                request.data_frame_with(request.default_words().iter().map(|w| w + shift)),
            )]
        })
        .await;
        let blocks = vec![block(100, 3), block(7, 2)];
        let device = connect_tuned(&args(address, "60s"), blocks, tuning()).await.unwrap();
        let handle = &device.handle;
        assert!(!handle.snapshot().available());

        assert!(changed_within(handle, 2000).await);
        while handle.snapshot().words(7, 2).is_none() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        // Drop the permit stored for the second block's words that may not have been waited for.
        tokio::time::sleep(Duration::from_millis(30)).await;
        let _ = changed_within(handle, 1).await;
        assert!(handle.snapshot().available());
        assert_eq!(handle.snapshot().words(100, 3), Some(vec![3100, 3101, 3102]));
        assert_eq!(handle.snapshot().get(8), Some(3008));
        assert!(!changed_within(handle, 200).await, "nothing has changed");

        offset.store(1, SeqCst);
        while handle.snapshot().get(8) != Some(3009) {
            assert!(changed_within(handle, 2000).await);
        }
        assert_eq!(handle.snapshot().get(100), Some(3101));

        let Device { handle, task } = device;
        let other = handle.clone();
        drop(handle);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!task.is_finished());
        drop(other);
        tokio::time::timeout(Duration::from_secs(2), task).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn the_words_of_a_read_are_stored_together() {
        // Two registers that always hold the same word, which changes with every read.
        let reads = Arc::new(AtomicU16::new(0));
        let (address, _server) = fake::start(move |request| {
            let n = reads.fetch_add(1, SeqCst);
            vec![Step::Send(request.data_frame_with([n; 2]))]
        })
        .await;
        let device = connect_tuned(&args(address, "60s"), vec![block(1, 2)], tuning()).await;
        let device = device.unwrap();
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(3)).await;
            if let Some(words) = device.handle.snapshot().words(1, 2) {
                assert_eq!(words[0], words[1]);
            }
        }
        device.task.abort();
    }

    #[tokio::test]
    async fn availability_follows_the_inverter_coming_and_going() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let serve = |listener| fake::serve(listener, Arc::default(), Arc::new(fake::ok));
        let server = serve(listener);
        let blocks = vec![block(1, 1)];
        let device = connect_tuned(&args(address, "300ms"), blocks, tuning()).await.unwrap();
        let handle = &device.handle;

        assert!(changed_within(handle, 2000).await);
        assert!(handle.snapshot().available());

        server.abort();
        let _ = server.await;
        let gone = Instant::now();
        assert!(changed_within(handle, 2000).await, "no notification of the inverter going away");
        assert!(!handle.snapshot().available());
        assert!(gone.elapsed() > Duration::from_millis(100), "{:?}", gone.elapsed());
        assert_eq!(handle.snapshot().get(1), Some(3001));

        let _server = serve(tokio::net::TcpListener::bind(address).await.unwrap());
        assert!(changed_within(handle, 3000).await, "no notification of the inverter coming back");
        assert!(handle.snapshot().available());
        device.task.abort();
    }

    #[tokio::test]
    async fn blocks_with_nothing_at_their_addresses_are_left_alone_for_a_while() {
        let (arrivals, respond) = recording(|request| match request.start {
            500 => vec![Step::Send(request.exception(2))],
            _ => fake::ok(request),
        });
        let (address, _server) = fake::start(respond).await;
        let blocks = vec![block(500, 10), block(1, 1)];
        let tuning = Tuning { retry_missing_every: Duration::from_millis(400), ..tuning() };
        let device = connect_tuned(&args(address, "60s"), blocks, tuning).await.unwrap();
        let count = |start| requests(&arrivals).iter().filter(|a| a.1 == start).count();

        tokio::time::sleep(Duration::from_millis(250)).await;
        // Every 20 milliseconds would be a dozen of them by now.
        assert_eq!(count(500), 1);
        assert!(count(1) >= 5, "{}", count(1));
        // The inverter is available because of the other block.
        assert!(device.handle.snapshot().available());
        assert_eq!(device.handle.snapshot().get(500), None);

        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!((2..=3).contains(&count(500)), "{}", count(500));
        device.task.abort();
    }

    #[tokio::test]
    async fn a_block_that_cannot_be_read_is_tried_again_with_increasing_delays() {
        // The connection is lost with the first four requests, and with the seventh and eighth.
        let (arrivals, respond) = recording(|request| match request.n {
            0..4 | 6 | 7 => vec![Step::Close],
            _ => fake::ok(request),
        });
        let (address, _server) = fake::start(respond).await;
        let period = Duration::from_millis(300);
        let blocks = vec![PollBlock { period, ..block(1, 2) }];
        let backoff = Backoff { base: Duration::from_millis(100), max: Duration::from_millis(250) };
        let tuning = Tuning { backoff, ..tuning() };
        let device = connect_tuned(&args(address, "60s"), blocks, tuning).await.unwrap();
        while arrivals.lock().unwrap().len() < 9 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        device.task.abort();
        let arrivals = arrivals.lock().unwrap();
        let gaps: Vec<_> = arrivals.windows(2).map(|w| w[1].0 - w[0].0).collect();
        let (ms, right_away) = (Duration::from_millis, Duration::from_millis(80));
        // Right away the first time, and then the backoff, with no delay on top of the schedule
        // for opening the connection anew.
        assert!(gaps[0] < right_away, "{gaps:?}");
        assert!(gaps[1] >= ms(90) && gaps[1] < ms(190), "{gaps:?}");
        assert!(gaps[2] >= ms(190) && gaps[2] < ms(390), "{gaps:?}");
        assert!(gaps[3] >= ms(240) && gaps[3] < ms(290), "{gaps:?}");
        assert!(gaps[4] >= period - ms(20) && gaps[4] < period + right_away, "{gaps:?}");
        assert!(gaps[5] >= period - ms(20), "{gaps:?}");
        assert!(gaps[6] < right_away, "{gaps:?}");
        assert!(gaps[7] >= ms(90) && gaps[7] < ms(190), "{gaps:?}");
    }

    #[tokio::test]
    async fn the_backoff_is_capped_by_the_period() {
        let (arrivals, respond) = recording(|_| vec![Step::Close]);
        let (address, _server) = fake::start(respond).await;
        let blocks = vec![PollBlock { period: Duration::from_millis(30), ..block(1, 2) }];
        let backoff = Backoff { base: Duration::from_millis(500), max: Duration::from_secs(5) };
        let tuning = Tuning { backoff, ..tuning() };
        let device = connect_tuned(&args(address, "60s"), blocks, tuning).await.unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        device.task.abort();
        // Without the period getting in the way there would be three.
        assert!(arrivals.lock().unwrap().len() >= 6, "{}", arrivals.lock().unwrap().len());
    }

    #[tokio::test]
    async fn exceptions_are_not_retried_right_away_and_do_not_make_the_inverter_available() {
        let (arrivals, respond) = recording(|request| vec![Step::Send(request.exception(4))]);
        let (address, _server) = fake::start(respond).await;
        let blocks = vec![PollBlock { period: Duration::from_millis(150), ..block(1, 2) }];
        let device = connect_tuned(&args(address, "60s"), blocks, tuning()).await.unwrap();
        assert!(!changed_within(&device.handle, 400).await);
        assert!(!device.handle.snapshot().available());
        device.task.abort();
        // At its period, and not any more eagerly because of it failing.
        let arrivals = arrivals.lock().unwrap().len();
        assert!((2..=3).contains(&arrivals), "{arrivals}");
    }

    async fn settled(handle: &Handle, start: u16, count: u16) {
        while handle.snapshot().words(start, count).is_none() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        let _ = changed_within(handle, 1).await;
    }

    fn once(start: u16, count: u16) -> PollBlock {
        PollBlock { period: Duration::from_secs(600), ..block(start, count) }
    }

    #[tokio::test]
    async fn a_write_is_one_request_followed_by_a_read_back() {
        let bank = Bank::default();
        let (arrivals, respond) = recording(bank.responder());
        let (address, _server) = fake::start(respond).await;
        let blocks = vec![once(0x1028, 4)];
        let device = connect_tuned(&args_allowing_writes(address), blocks, tuning()).await.unwrap();
        let handle = &device.handle;
        assert!(handle.writes_allowed());
        settled(handle, 0x1028, 4).await;
        assert_eq!(handle.snapshot().words(0x1029, 2), Some(vec![7137, 7138]));

        handle.write(0x1029, vec![1, 2]).await.unwrap();
        assert_eq!((bank.get(0x1029), bank.get(0x102A)), (Some(1), Some(2)));
        assert_eq!(handle.snapshot().words(0x1028, 4), Some(vec![7136, 1, 2, 7139]));
        assert!(changed_within(handle, 1000).await);

        let log = arrivals.lock().unwrap();
        let writes: Vec<_> = log.iter().filter(|(_, r)| r.is_write()).collect();
        assert_eq!(writes.len(), 1);
        assert_eq!((writes[0].1.start, writes[0].1.count), (0x1029, 2));
        assert_eq!(writes[0].1.written, [1, 2]);
        // Followed by the read of the same registers, and nothing else in between.
        let at = log.iter().position(|(_, r)| r.is_write()).unwrap();
        assert_eq!(
            (log[at + 1].1.function, log[at + 1].1.start, log[at + 1].1.count),
            (3, 0x1029, 2)
        );
        assert_eq!(log.len(), at + 2);
        drop(log);
        device.task.abort();
    }

    #[tokio::test]
    async fn a_long_write_is_read_back_in_parts() {
        let bank = Bank::default();
        let (arrivals, respond) = recording(bank.responder());
        let (address, _server) = fake::start(respond).await;
        let device = connect_tuned(&args_allowing_writes(address), vec![], tuning()).await.unwrap();
        let words: Vec<u16> = (0..100).collect();
        device.handle.write(0x2000, words.clone()).await.unwrap();
        assert_eq!(device.handle.snapshot().words(0x2000, 100), Some(words));
        assert_eq!(requests(&arrivals), [(16, 0x2000), (3, 0x2000), (3, 0x2040)]);
        device.task.abort();
    }

    #[tokio::test]
    async fn a_write_makes_the_inverter_available() {
        let bank = Bank::default();
        let (address, _server) = fake::start(bank.responder()).await;
        let device = connect_tuned(&args_allowing_writes(address), vec![], tuning()).await.unwrap();
        assert!(!device.handle.snapshot().available());
        device.handle.write(5, vec![1]).await.unwrap();
        assert!(device.handle.snapshot().available());
        device.task.abort();
    }

    #[tokio::test]
    async fn a_write_is_refused_unless_allowed() {
        let (arrivals, respond) = recording(fake::ok);
        let (address, _server) = fake::start(respond).await;
        let device = connect_tuned(&args(address, "60s"), vec![once(1, 2)], tuning()).await;
        let device = device.unwrap();
        let handle = &device.handle;
        assert!(!handle.writes_allowed());
        settled(handle, 1, 2).await;

        let refused = handle.write(1, vec![5, 6]).await;
        assert!(matches!(refused, Err(WriteError::NotAllowed)), "{refused:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(requests(&arrivals), [(3, 1)]);
        assert_eq!(handle.snapshot().words(1, 2), Some(vec![3001, 3002]));
        device.task.abort();

        let detached = Handle::detached(Snapshot::new());
        assert!(matches!(detached.write(1, vec![5]).await, Err(WriteError::NotAllowed)));
    }

    #[tokio::test]
    async fn a_value_that_is_not_taken_is_read_back() {
        let bank = Bank::default();
        bank.set(0x1029, 100);
        bank.set(0x102A, 200);
        bank.ignore_writes(true);
        let (address, _server) = fake::start(bank.responder()).await;
        let device = connect_tuned(&args_allowing_writes(address), vec![], tuning()).await.unwrap();
        let handle = &device.handle;

        assert_eq!(handle.write(0x1029, vec![1, 2]).await.unwrap(), [100, 200]);
        assert_eq!(bank.get(0x1029), Some(100));
        assert_eq!(handle.snapshot().words(0x1029, 2), Some(vec![100, 200]));
        assert!(changed_within(handle, 1000).await);
        device.task.abort();
    }

    #[tokio::test]
    async fn a_write_that_fails_is_reported_and_leaves_the_snapshot_alone() {
        let (address, _server) = fake::start(|request| match request.is_write() {
            true => vec![Step::Send(request.exception(3))],
            false => fake::ok(request),
        })
        .await;
        let blocks = vec![once(0x1029, 2)];
        let device = connect_tuned(&args_allowing_writes(address), blocks, tuning()).await.unwrap();
        settled(&device.handle, 0x1029, 2).await;

        let failed = device.handle.write(0x1029, vec![1, 2]).await;
        let is_exception =
            matches!(&failed, Err(WriteError::Request(RequestError::Exception(ExceptionCode(3)))));
        assert!(is_exception, "{failed:?}");
        let empty = device.handle.write(0x1029, vec![]).await;
        assert!(matches!(empty, Err(WriteError::Request(RequestError::InvalidRange { .. }))));
        assert_eq!(device.handle.snapshot().words(0x1029, 2), Some(vec![7137, 7138]));
        assert!(!changed_within(&device.handle, 100).await);
        device.task.abort();
    }

    #[tokio::test]
    async fn writes_are_served_before_the_polls_that_are_due() {
        let bank = Bank::default();
        // The first poll takes a while, and the other one is due by the time it is over.
        let (arrivals, respond) = recording(move |request| match request.n {
            0 => [Step::Wait(Duration::from_millis(100))]
                .into_iter()
                .chain(bank.respond(request))
                .collect(),
            _ => bank.respond(request),
        });
        let (address, _server) = fake::start(respond).await;
        let blocks = vec![once(10, 1), once(20, 1)];
        let device = connect_tuned(&args_allowing_writes(address), blocks, tuning()).await.unwrap();

        tokio::time::sleep(Duration::from_millis(30)).await;
        // The poll is still under way: it is not given up on.
        assert_eq!(requests(&arrivals), [(3, 10)]);
        device.handle.write(0x1029, vec![1]).await.unwrap();
        while arrivals.lock().unwrap().len() < 4 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(requests(&arrivals), [(3, 10), (16, 0x1029), (3, 0x1029), (3, 20)]);
        device.task.abort();
    }

    #[tokio::test]
    async fn read_once_reads_every_block_and_reports_those_that_failed() {
        let (arrivals, respond) = recording(|request| match request.start {
            500 => vec![Step::Send(request.exception(2))],
            600 => vec![Step::Close],
            _ => fake::ok(request),
        });
        let (address, _server) = fake::start(respond).await;
        let blocks = [block(1, 2), block(500, 3), block(600, 1), block(100, 70)];
        let (snapshot, failed) = read_once(&args(address, "60s"), &blocks).await.unwrap();
        assert!(snapshot.available());
        assert_eq!(snapshot.words(1, 2), Some(vec![3001, 3002]));
        assert_eq!(snapshot.words(100, 70).map(|w| w.len()), Some(70));
        assert_eq!(snapshot.get(500), None);
        let failed: Vec<_> = failed.iter().map(|(b, e)| (b.start, b.count, e)).collect();
        assert!(matches!(
            failed[..],
            [
                (500, 3, RequestError::Exception(ExceptionCode(2))),
                (600, 1, RequestError::Transport(_))
            ]
        ));
        assert_eq!(requests(&arrivals), [(3, 1), (3, 500), (3, 600), (3, 100), (3, 164)]);
    }

    #[tokio::test]
    async fn read_once_of_nothing_is_not_available() {
        let (address, _server) =
            fake::start(|request| vec![Step::Send(request.exception(2))]).await;
        let (snapshot, failed) = read_once(&args(address, "60s"), &[block(1, 1)]).await.unwrap();
        assert!(!snapshot.available());
        assert_eq!(failed.len(), 1);
    }
}
