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
    handler: Option<thread::JoinHandle<()>>,
}

impl Worker {
    pub fn create<A>(
        addr: A,
        conn_settings: ConnectionSettings,
        receiver: Arc<Mutex<Receiver<Message>>>,
        flush_period: Duration,
        flush_size: usize,
    ) -> io::Result<Worker>
    where
        A: ToSocketAddrs + Clone + Debug + Send + 'static,
    {
        let mut stream: Stream<A, TcpStream> = Stream::connect(addr, conn_settings)?;

        let emitters = RefCell::new(HashMap::new());
        let wh = WorkerHandler { emitters };

        let builder = thread::Builder::new().name("fluent-worker-pool".to_owned());
        let handler = builder
            .spawn(move || {
                let mut start = Instant::now();
                loop {
                    let receiver = receiver.lock().expect("Couldn't be locked.");
                    match receiver.recv_timeout(flush_period) {
                        Ok(msg) => match msg {
                            Message::Queuing(tag, tm, msg) => {
                                trace!("Received a queuing message; tag: {}.", tag);
                                wh.push(tag, tm, msg);
                                if start.elapsed() >= flush_period {
                                    trace!("Started flushing messages.");
                                    wh.flush(&mut stream, Some(flush_size));
                                    start = Instant::now();
                                    trace!("Done flushing messages.");
                                }
                            }
                            Message::Terminate => {
                                info!("Received a terminate message.");
                                wh.flush(&mut stream, None);
                                match stream.stream.borrow_mut().shutdown(Shutdown::Both) {
                                    Ok(_) => (),
                                    Err(e) => {
                                        error!("Occurred during terminating worker: {:?}.", e);
                                    }
                                }
                                break;
                            }
                        },
                        Err(_) => {
                            trace!("Start force flush messages.");
                            wh.flush(&mut stream, Some(flush_size));
                            start = Instant::now();
                        }
                    };
                }
            })
            .ok();

        Ok(Worker { handler })
    }

    pub fn join_handler(&mut self) {
        if let Some(h) = self.handler.take() {
            h.join().expect("Couldn't join on the associated thread");
        }
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
        RW: Reconnect + WriteRead,
    {
        for (tag, emitter) in self.emitters.borrow().iter() {
            if let Err(err) = emitter.emit(rw, size) {
                error!(
                    "Tag '{}' unexpected error occurred during emitting message, cause: '{:?}'.",
                    tag, err
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
            addr.clone(),
            settings,
            Arc::clone(&receiver),
            Duration::from_millis(1),
            1,
        );
        assert!(ret.is_err())
    }
}
