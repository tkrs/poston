use crate::connect::*;
use crate::emitter::Emitter;
use crossbeam_channel::Receiver;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Debug;
use std::io;
use std::net::{Shutdown, TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

pub struct Worker {
    pub id: usize,
    pub handler: Option<thread::JoinHandle<()>>,
}

impl Worker {
    pub fn create<A>(
        id: usize,
        addr: A,
        conn_settings: ConnectionSettings,
        receiver: Arc<Mutex<Receiver<Message>>>,
        flush_period: Duration,
        flush_size: usize,
    ) -> io::Result<Worker>
    where
        A: ToSocketAddrs + Clone + Debug,
        A: Send + 'static,
    {
        let mut stream: Stream<A, TcpStream> = Stream::connect(addr, conn_settings)?;

        let emitters = RefCell::new(HashMap::new());
        let wh = WorkerHandler { id, emitters };

        let builder = thread::Builder::new().name(format!("fluent-worker-pool-{}", id));
        let handler = builder
            .spawn(move || {
                let mut start = Instant::now();
                loop {
                    let receiver = receiver.lock().expect("Couldn't be locked.");
                    match receiver.recv_timeout(flush_period) {
                        Ok(msg) => match msg {
                            Message::Queuing(tag, tm, msg) => {
                                trace!("Worker {} received a queuing message; tag: {}.", id, tag);
                                wh.push(tag, tm, msg);
                                if start.elapsed() >= flush_period {
                                    trace!("Worker {} started flushing messages.", id);
                                    wh.flush(&mut stream, Some(flush_size));
                                    start = Instant::now();
                                    trace!("Worker {} done flushing messages.", id);
                                }
                            }
                            Message::Terminate => {
                                info!("Worker {} received a terminate message.", id);
                                wh.flush(&mut stream, None);
                                match stream.stream.borrow_mut().shutdown(Shutdown::Both) {
                                    Ok(_) => (),
                                    Err(e) => {
                                        error!(
                                            "Worker {} occurred during terminating worker: {:?}.",
                                            id, e
                                        );
                                    }
                                }
                                break;
                            }
                        },
                        Err(_) => {
                            trace!("Worker {} start force flush messages.", id);
                            wh.flush(&mut stream, Some(flush_size));
                            start = Instant::now();
                        }
                    };
                }
            })
            .ok();

        Ok(Worker { id, handler })
    }
}

struct WorkerHandler {
    id: usize,
    emitters: RefCell<HashMap<String, Emitter>>,
}

impl WorkerHandler {
    fn push(&self, tag: String, tm: SystemTime, msg: Vec<u8>) {
        let mut emitters = self.emitters.borrow_mut();
        let emitter = emitters
            .entry(tag.clone())
            .or_insert_with(|| Emitter::new(self.id, tag));
        emitter.push((tm, msg));
    }

    fn flush<RW>(&self, rw: &mut RW, size: Option<usize>)
    where
        RW: Reconnect + WriteRead,
    {
        for emitter in self.emitters.borrow().values() {
            emitter.emit(rw, size)
        }
    }
}

pub enum Message {
    Queuing(String, SystemTime, Vec<u8>),
    Terminate,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connect::ConnectionSettings;
    use crossbeam_channel::unbounded;

    #[test]
    fn worker_create_should_return_err_when_the_connection_open_is_failed() {
        let addr = "127.0.0.1:25";
        let settings = ConnectionSettings {
            connect_retry_initial_delay: Duration::new(0, 1),
            connect_retry_max_delay: Duration::new(0, 1),
            connect_retry_timeout: Duration::from_millis(10),
            ..Default::default()
        };
        let (_, receiver) = unbounded();
        let receiver = Arc::new(Mutex::new(receiver));
        let ret = Worker::create(
            1,
            addr.clone(),
            settings,
            Arc::clone(&receiver),
            Duration::from_millis(1),
            1,
        );
        assert!(ret.is_err())
    }
}
