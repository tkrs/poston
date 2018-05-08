use connect::{ConnectionSettings, ReconnectWrite, Stream};
use emitter::Emitter;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Read;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

pub struct Worker {
    pub id: usize,
    pub handler: Option<thread::JoinHandle<()>>,
}

impl Worker {
    pub fn new<A>(
        id: usize,
        addr: A,
        conn_settings: ConnectionSettings,
        receiver: Arc<Mutex<mpsc::Receiver<Message>>>,
        flush_period: Duration,
        flush_size: usize,
    ) -> Worker
    where
        A: ToSocketAddrs + Clone,
        A: Send + 'static,
    {
        let mut stream: Stream<A, TcpStream> = Stream::connect(addr, conn_settings)
            .expect(&format!("Worker {} couldn't connect to server.", id));

        let emitters = RefCell::new(HashMap::new());
        let wh = WorkerHandler { emitters };

        let builder = thread::Builder::new().name(format!("fluent-worker-pool-{}", id));
        let handler = builder
            .spawn(move || {
                let mut start = Instant::now();
                loop {
                    let receiver = receiver.lock().expect("Couldn't be locked.");
                    match receiver.recv_timeout(flush_period) {
                        Ok(msg) => match msg {
                            Message::Queuing(tag, tm, msg) => {
                                wh.push(tag, tm, msg);
                                if start.elapsed() >= flush_period {
                                    wh.flush(&mut stream, Some(flush_size));
                                    start = Instant::now();
                                }
                            },
                            Message::Terminate => {
                                wh.flush(&mut stream, None);
                                break;
                            }
                        },
                        Err(_) => {
                            wh.flush(&mut stream, Some(flush_size));
                            start = Instant::now();
                        }
                    };
                }
            })
            .ok();

        Worker { id, handler }
    }
}

struct WorkerHandler {
    emitters: RefCell<HashMap<String, Emitter>>,
}

impl WorkerHandler {
    fn push(&self, tag: String, tm: SystemTime, msg: Vec<u8>) {
        let mut emitters = self.emitters.borrow_mut();
        let emitter = emitters
            .entry(tag.clone())
            .or_insert_with(|| Emitter::new(tag));
        emitter.push((tm, msg));
    }

    fn flush<RW>(&self, rw: &mut RW, size: Option<usize>)
    where
        RW: ReconnectWrite + Read,
    {
        for (tag, emitter) in self.emitters.borrow().iter() {
            if let Err(e) = emitter.emit(rw, size) {
                error!(
                    "Tag: {}, an unexpected error occurred during emitting message: Caused by '{:?}'.",
                    tag, e
                );
            }
        }
    }
}

pub enum Message {
    Queuing(String, SystemTime, Vec<u8>),
    Terminate,
}

#[cfg(test)]
mod test {}
