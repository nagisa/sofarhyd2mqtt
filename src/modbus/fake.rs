use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{JoinHandle, JoinSet};

#[derive(Clone, Debug)]
pub(crate) struct Req {
    pub n: usize,
    pub transaction: u16,
    pub unit: u8,
    pub function: u8,
    pub start: u16,
    pub count: u16,
    pub written: Vec<u16>,
}

pub(crate) enum Step {
    Send(Vec<u8>),
    Wait(Duration),
    Close,
}

pub(crate) fn mbap(transaction: u16, protocol: u16, unit: u8, pdu: &[u8]) -> Vec<u8> {
    let mut frame = Vec::from(transaction.to_be_bytes());
    frame.extend(protocol.to_be_bytes());
    frame.extend((pdu.len() as u16 + 1).to_be_bytes());
    frame.push(unit);
    frame.extend(pdu);
    frame
}

/// The word that a register holds unless it has been written: `address + 3000`.
pub(crate) fn default_word(address: u16) -> u16 {
    address.wrapping_add(3000)
}

impl Req {
    pub fn is_write(&self) -> bool {
        self.function == 0x10
    }

    pub fn default_words(&self) -> Vec<u16> {
        (0..self.count).map(|i| default_word(self.start.wrapping_add(i))).collect()
    }

    /// As if it were the response to an older request.
    pub fn with_transaction(&self, transaction: u16) -> Req {
        Req { transaction, ..self.clone() }
    }

    pub fn frame_from(&self, unit: u8, pdu: &[u8]) -> Vec<u8> {
        mbap(self.transaction, 0, unit, pdu)
    }

    pub fn exception(&self, code: u8) -> Vec<u8> {
        self.frame_from(self.unit, &[self.function | 0x80, code])
    }

    pub fn data_frame_with(&self, words: impl IntoIterator<Item = u16>) -> Vec<u8> {
        let words: Vec<u8> = words.into_iter().flat_map(u16::to_be_bytes).collect();
        let mut pdu = vec![self.function, words.len() as u8];
        pdu.extend(words);
        self.frame_from(self.unit, &pdu)
    }

    pub fn data_frame(&self) -> Vec<u8> {
        self.data_frame_with(self.default_words())
    }

    pub fn echo_frame_with(&self, start: u16, count: u16) -> Vec<u8> {
        let mut pdu = vec![self.function];
        pdu.extend(start.to_be_bytes());
        pdu.extend(count.to_be_bytes());
        self.frame_from(self.unit, &pdu)
    }

    pub fn echo_frame(&self) -> Vec<u8> {
        self.echo_frame_with(self.start, self.count)
    }
}

pub(crate) fn ok(request: &Req) -> Vec<Step> {
    let frame = if request.is_write() { request.echo_frame() } else { request.data_frame() };
    vec![Step::Send(frame)]
}

#[derive(Clone, Default)]
pub(crate) struct Bank {
    words: Arc<Mutex<BTreeMap<u16, u16>>>,
    ignore_writes: Arc<AtomicBool>,
}

impl Bank {
    pub fn get(&self, address: u16) -> Option<u16> {
        self.words.lock().unwrap().get(&address).copied()
    }

    pub fn set(&self, address: u16, word: u16) {
        self.words.lock().unwrap().insert(address, word);
    }

    /// As an inverter does with values it does not like.
    pub fn ignore_writes(&self, ignore: bool) {
        self.ignore_writes.store(ignore, SeqCst);
    }

    pub fn respond(&self, request: &Req) -> Vec<Step> {
        let mut words = self.words.lock().unwrap();
        if request.is_write() {
            if !self.ignore_writes.load(SeqCst) {
                for (i, &word) in request.written.iter().enumerate() {
                    words.insert(request.start + i as u16, word);
                }
            }
            return vec![Step::Send(request.echo_frame())];
        }
        let read = (0..request.count).map(|i| {
            let address = request.start.wrapping_add(i);
            words.get(&address).copied().unwrap_or(default_word(address))
        });
        vec![Step::Send(request.data_frame_with(read))]
    }
}

impl Bank {
    pub fn responder(&self) -> impl Fn(&Req) -> Vec<Step> + Send + Sync + 'static {
        let bank = self.clone();
        move |request| bank.respond(request)
    }
}

pub(crate) type Respond = Arc<dyn Fn(&Req) -> Vec<Step> + Send + Sync>;

pub(crate) async fn start_counting(
    accepted: Arc<AtomicUsize>,
    respond: impl Fn(&Req) -> Vec<Step> + Send + Sync + 'static,
) -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    (listener.local_addr().unwrap(), serve(listener, accepted, Arc::new(respond)))
}

pub(crate) async fn start(
    respond: impl Fn(&Req) -> Vec<Step> + Send + Sync + 'static,
) -> (SocketAddr, JoinHandle<()>) {
    start_counting(Arc::default(), respond).await
}

pub(crate) fn serve(
    listener: TcpListener,
    accepted: Arc<AtomicUsize>,
    respond: Respond,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let (mut connections, requests) = (JoinSet::new(), Arc::new(AtomicUsize::new(0)));
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            accepted.fetch_add(1, SeqCst);
            let (requests, respond) = (Arc::clone(&requests), Arc::clone(&respond));
            connections.spawn(serve_connection(socket, requests, respond));
        }
    })
}

async fn serve_connection(mut socket: TcpStream, requests: Arc<AtomicUsize>, respond: Respond) {
    let word = |b: &[u8]| u16::from_be_bytes([b[0], b[1]]);
    loop {
        let mut header = [0; 7];
        if socket.read_exact(&mut header).await.is_err() {
            return;
        }
        let (transaction, protocol, length) =
            (word(&header), word(&header[2..]), word(&header[4..]));
        assert_eq!(protocol, 0, "bad protocol in {header:02x?}");
        let mut pdu = vec![0; usize::from(length) - 1];
        if socket.read_exact(&mut pdu).await.is_err() {
            return;
        }
        let (unit, function) = (header[6], pdu[0]);
        let (start, count) = (word(&pdu[1..]), word(&pdu[3..]));
        let written = match function {
            3 => {
                assert_eq!(pdu.len(), 5, "bad read {pdu:02x?}");
                Vec::new()
            }
            0x10 => {
                assert_eq!(pdu.len(), 6 + 2 * usize::from(count), "bad write {pdu:02x?}");
                assert_eq!(usize::from(pdu[5]), 2 * usize::from(count), "bad write {pdu:02x?}");
                pdu[6..].chunks_exact(2).map(word).collect()
            }
            function => panic!("request with function code {function}"),
        };
        let n = requests.fetch_add(1, SeqCst);
        let request = Req { n, transaction, unit, function, start, count, written };
        for step in respond(&request) {
            match step {
                Step::Send(bytes) => {
                    if socket.write_all(&bytes).await.is_err() {
                        return;
                    }
                }
                Step::Wait(duration) => tokio::time::sleep(duration).await,
                Step::Close => return,
            }
        }
    }
}
