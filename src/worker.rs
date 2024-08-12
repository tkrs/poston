use crate::connect::*;
use crate::queue::*;
use crossbeam_channel::{Receiver, Sender};
use std::collections::HashMap;
use std::fmt::Debug;
use std::io;
use std::net::{TcpStream, ToSocketAddrs};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

pub struct Worker {
    handler: Option<thread::JoinHandle<()>>,
}

impl Worker {
    pub fn create<A>(
        addr: A,
        conn_settings: ConnectionSettings,
        receiver: Receiver<Message>,
        flush_period: Duration,
        flush_size: usize,
    ) -> io::Result<Self>
    where
        A: ToSocketAddrs + Clone + Debug + Send + 'static,
    {
        let mut stream: Stream<A, TcpStream> = Stream::connect(addr, conn_settings)?;
        let thread_builder = thread::Builder::new().name("fluent-worker-pool".to_owned());
        let handler = thread_builder.spawn(move || {
            start_worker(&mut stream, receiver, flush_period, flush_size);
            stream
                .close()
                .unwrap_or_else(|e| panic!("Failed to shutdown the stream: {:?}", e));
        })?;

        Ok(Self {
            handler: Some(handler),
        })
    }

    pub fn join_handler(&mut self) {
        if let Some(h) = self.handler.take() {
            h.join().expect("Couldn't join on the associated thread");
        }
    }
}

pub enum Message {
    Queuing(String, SystemTime, Vec<u8>),
    Terminating(Sender<()>),
}

fn start_worker<S: WriteRead>(
    stream: S,
    receiver: Receiver<Message>,
    flush_period: Duration,
    flush_size: usize,
) {
    let emitters = HashMap::new();
    let mut queue = QueueHandler {
        emitters,
        flusher: stream,
    };
    let mut now = Instant::now();

    loop {
        match receiver.recv_timeout(flush_period) {
            Ok(msg) => match handle_message(msg, &now, flush_period, flush_size, &mut queue) {
                HandleResult::Queued => (),
                HandleResult::Flushed => now = Instant::now(),
                HandleResult::Terminated => break,
            },
            Err(_) => {
                trace!("Start force flush messages");
                queue.flush(Some(flush_size));
                now = Instant::now();
            }
        };
    }
}

fn handle_message(
    msg: Message,
    now: &Instant,
    flush_period: Duration,
    flush_size: usize,
    queue: &mut dyn Queue,
) -> HandleResult {
    match msg {
        Message::Queuing(tag, tm, msg) => {
            trace!(
                "Received a queuing message, tag: {}, time: {:?}, size: {}",
                tag,
                tm,
                msg.len()
            );
            queue.push(tag, tm, msg);
            if now.elapsed() >= flush_period {
                trace!("Start flushing messages");
                queue.flush(Some(flush_size));
                trace!("Flushed messages");
                HandleResult::Flushed
            } else {
                HandleResult::Queued
            }
        }
        Message::Terminating(sender) => {
            info!("Received a terminating message");
            queue.flush(None);
            info!("Flushed, queue remaining: {}", queue.len());
            sender.send(()).unwrap();
            HandleResult::Terminated
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connect::ConnectionSettings;
    use crossbeam_channel::{bounded, unbounded};

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
        let ret = Worker::create(addr, settings, receiver, Duration::from_millis(1), 1);
        assert!(ret.is_err())
    }

    struct Q;
    impl Queue for Q {
        fn push(&mut self, _tag: String, _tm: SystemTime, _msg: Vec<u8>) {}
        fn flush(&mut self, _size: Option<usize>) {}
        fn len(&self) -> usize {
            0
        }
    }

    #[test]
    fn test_handle_message_queued() {
        let msg = Message::Queuing("tag".into(), SystemTime::now(), vec![1, 2, 4]);
        let mut now = Instant::now();
        let flush_period = Duration::from_secs(100);
        let flush_size = 2;
        let mut queue = Q;

        assert_eq!(
            handle_message(msg, &mut now, flush_period, flush_size, &mut queue),
            HandleResult::Queued
        );
    }

    #[test]
    fn test_handle_message_flushed() {
        let now = Instant::now();
        let flush_period = Duration::from_nanos(1);

        assert_eq!(
            handle_message(
                Message::Queuing("tag".into(), SystemTime::now(), vec![1, 2, 4]),
                &mut (now - flush_period),
                flush_period,
                1,
                &mut Q
            ),
            HandleResult::Flushed
        );
    }

    #[test]
    fn test_handle_message_terminated() {
        let (sender, receiver) = bounded::<()>(1);
        assert_eq!(
            handle_message(
                Message::Terminating(sender),
                &mut Instant::now(),
                Duration::from_nanos(1),
                1,
                &mut Q
            ),
            HandleResult::Terminated
        );
        receiver.recv_timeout(Duration::from_millis(100)).unwrap();
    }

    struct WR;
    impl WriteRead for WR {
        fn write_and_read(&mut self, _buf: &[u8], _chunk: &str) -> Result<(), crate::error::Error> {
            Ok(())
        }
    }

    #[test]
    fn test_start_worker_terminate() {
        let (sender, receiver) = unbounded();
        let (sender2, receiver2) = bounded::<()>(1);

        thread::spawn(move || start_worker(WR, receiver, Duration::from_nanos(1), 1));
        sender.send(Message::Terminating(sender2)).unwrap();

        receiver2.recv_timeout(Duration::from_millis(100)).unwrap();
    }
}
