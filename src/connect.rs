use crate::buffer;
use crate::error::Error;
use backoff::{Error as RetryError, ExponentialBackoff, Operation};
use std::cell::RefCell;
use std::fmt::Debug;
use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::thread;
use std::time::Duration;

pub trait Connect<T> {
    fn connect<A>(addr: A, settings: ConnectionSettings) -> io::Result<T>
    where
        A: ToSocketAddrs + Clone + Debug;
}

pub trait Reconnect {
    fn reconnect(&mut self) -> io::Result<()>;
}

pub trait WriteRead {
    fn write_and_read(&mut self, buf: &[u8], chunk: &str) -> Result<(), Error>;
}

#[derive(Debug)]
pub struct Stream<A, S> {
    pub addr: A,
    pub stream: RefCell<S>,
    pub settings: ConnectionSettings,
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct ConnectionSettings {
    pub connect_retry_initial_delay: Duration,
    pub connect_retry_max_delay: Duration,
    pub connect_retry_timeout: Duration,
    pub write_timeout: Duration,
    pub read_timeout: Duration,

    pub write_retry_timeout: Duration,
    pub write_retry_max_delay: Duration,
    pub write_retry_initial_delay: Duration,

    pub read_retry_timeout: Duration,
    pub read_retry_max_delay: Duration,
    pub read_retry_initial_delay: Duration,
}

impl<A, S> Stream<A, S>
where
    A: ToSocketAddrs + Clone + Debug,
    S: Connect<S>,
{
    pub fn connect(addr: A, settings: ConnectionSettings) -> io::Result<Stream<A, S>> {
        let stream = connect_with_retry(addr.clone(), settings)?;
        let stream = RefCell::new(stream);
        Ok(Self {
            addr,
            stream,
            settings,
        })
    }

    fn write_retry_initial_delay(&self) -> Duration {
        self.settings.write_retry_initial_delay
    }
    fn write_retry_timeout(&self) -> Duration {
        self.settings.write_retry_timeout
    }
    fn write_retry_max_delay(&self) -> Duration {
        self.settings.write_retry_max_delay
    }

    fn read_retry_initial_delay(&self) -> Duration {
        self.settings.read_retry_initial_delay
    }
    fn read_retry_timeout(&self) -> Duration {
        self.settings.read_retry_timeout
    }
    fn read_retry_max_delay(&self) -> Duration {
        self.settings.read_retry_max_delay
    }
}

impl<A, S> Reconnect for Stream<A, S>
where
    A: ToSocketAddrs + Clone + Debug,
    S: Connect<S>,
{
    fn reconnect(&mut self) -> io::Result<()> {
        debug!("Start reconnect()");
        let stream = connect_with_retry(self.addr.clone(), self.settings)?;
        *self.stream.borrow_mut() = stream;
        debug!("End reconnect()");
        Ok(())
    }
}

impl<A, S> Write for Stream<A, S>
where
    S: Write,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.stream.borrow_mut().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.stream.borrow_mut().flush()
    }
}

impl<A, S> Read for Stream<A, S>
where
    S: Read,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.stream.borrow_mut().read(buf)
    }
}

impl<A, S> WriteRead for &mut Stream<A, S>
where
    A: ToSocketAddrs + Clone + Debug,
    S: Connect<S> + Read + Write,
{
    fn write_and_read(&mut self, buf: &[u8], chunk: &str) -> Result<(), Error> {
        let mut backoff = ExponentialBackoff {
            current_interval: self.write_retry_initial_delay(),
            initial_interval: self.write_retry_initial_delay(),
            max_interval: self.write_retry_max_delay(),
            max_elapsed_time: Some(self.write_retry_timeout()),
            ..Default::default()
        };

        let mut op = || {
            self.write(buf).map_err(|e| {
                warn!("Failed to write message, chunk: {}. cause: {:?}", chunk, e);
                if let Err(err) = self.reconnect() {
                    warn!("Failed to reconnect: {:?}", err);
                }
                RetryError::Transient(Error::NetworkError(e.to_string()))
            })?;

            let mut read_backoff = ExponentialBackoff {
                current_interval: self.read_retry_initial_delay(),
                initial_interval: self.read_retry_initial_delay(),
                max_interval: self.read_retry_max_delay(),
                max_elapsed_time: Some(self.read_retry_timeout()),
                ..Default::default()
            };

            let mut resp_buf = [0u8; 64];

            let mut read_op = || {
                self.read(&mut resp_buf).map_err(|e| {
                    debug!("Failed to read response, chunk: {}, cause: {:?}", chunk, e);
                    if e.kind() == io::ErrorKind::WouldBlock {
                        RetryError::Transient(Error::NetworkError(e.to_string()))
                    } else {
                        RetryError::Permanent(Error::NetworkError(e.to_string()))
                    }
                })
            };

            let read_size = read_op.retry(&mut read_backoff).map_err(|e| {
                warn!("Failed to read response, chunk: {}, cause: {:?}", chunk, e);
                match e {
                    RetryError::Permanent(e) => RetryError::Transient(e),
                    err => err,
                }
            })?;

            // It seems that `read()` returns `Ok(0)` while Fluentd is reloading configuration, and
            // it takes a few seconds to be restarted them. Also even if the same message retry
            // writing, 0 is returned unless the connection is reconnected.
            if read_size == 0 {
                warn!("Received empty response, chunk: {}", chunk);
                thread::sleep(Duration::from_secs(5));
                if let Err(err) = self.reconnect() {
                    warn!("Failed to reconnect: {:?}", err);
                }
                Err(RetryError::Transient(Error::NoAckResponseError))
            } else {
                let reply =
                    buffer::unpack_response(&resp_buf, read_size).map_err(RetryError::Transient)?;
                if reply.ack == chunk {
                    Ok(())
                } else {
                    warn!(
                        "Did not match ack and chunk, ack: {}, chunk: {}",
                        reply.ack, chunk
                    );
                    Err(RetryError::Transient(Error::AckUmatchedError(
                        reply.ack,
                        chunk.to_string(),
                    )))
                }
            }
        };

        op.retry(&mut backoff).map_err(|e| match e {
            RetryError::Permanent(err) | RetryError::Transient(err) => err,
        })
    }
}

impl Connect<TcpStream> for TcpStream {
    fn connect<A>(addr: A, settings: ConnectionSettings) -> io::Result<TcpStream>
    where
        A: ToSocketAddrs + Clone + Debug,
    {
        TcpStream::connect(addr).map(|s| {
            s.set_nodelay(true).unwrap();
            s.set_read_timeout(Some(settings.read_timeout)).unwrap();
            s.set_write_timeout(Some(settings.write_timeout)).unwrap();
            s
        })
    }
}

fn connect_with_retry<C, A>(addr: A, settings: ConnectionSettings) -> io::Result<C>
where
    A: ToSocketAddrs + Clone + Debug,
    C: Connect<C>,
{
    let mut backoff = ExponentialBackoff {
        current_interval: settings.connect_retry_initial_delay,
        initial_interval: settings.connect_retry_initial_delay,
        max_interval: settings.connect_retry_max_delay,
        max_elapsed_time: Some(settings.connect_retry_timeout),
        ..Default::default()
    };

    let mut op = || {
        let addr = addr.clone();
        debug!("Start connect to {:?}", addr);
        C::connect(&addr, settings).map_err(|err| {
            warn!("Failed to connect to {:?}", addr);
            RetryError::Transient(err)
        })
    };

    op.retry(&mut backoff).map_err(|e| match e {
        RetryError::Permanent(err) | RetryError::Transient(err) => {
            error!("Failed to connect to server, cause: {:?}", err);
            err
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{io, Duration, ToSocketAddrs};
    use super::{Connect, ConnectionSettings, Reconnect, Stream};
    use lazy_static::lazy_static;
    use std::collections::VecDeque;
    use std::convert::From;
    use std::io::{Read, Write};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    #[derive(Debug)]
    pub struct TestStream(AtomicUsize);

    mod s {
        use super::*;

        static CONN_COUNT: AtomicUsize = AtomicUsize::new(1);

        impl Connect<TestStream> for TestStream {
            fn connect<A>(addr: A, _s: ConnectionSettings) -> io::Result<TestStream>
            where
                A: ToSocketAddrs + Clone,
            {
                if let Ok(_) = addr.to_socket_addrs() {
                    let count = CONN_COUNT.fetch_add(1, Ordering::SeqCst);
                    if count % 20 == 0 {
                        Ok(TestStream(AtomicUsize::new(1)))
                    } else {
                        Err(io::Error::from(io::ErrorKind::ConnectionRefused))
                    }
                } else {
                    Err(io::Error::from(io::ErrorKind::ConnectionRefused))
                }
            }
        }
        impl Write for TestStream {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                let c = self.0.fetch_add(1, Ordering::SeqCst);
                if c == 10 {
                    Ok(buf.len())
                } else {
                    Err(io::Error::from(io::ErrorKind::TimedOut))
                }
            }
            fn flush(&mut self) -> io::Result<()> {
                Err(io::Error::from(io::ErrorKind::TimedOut))
            }
        }
        impl Read for TestStream {
            fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
                Err(io::Error::from(io::ErrorKind::WouldBlock))
            }
        }
    }

    #[test]
    fn connect() {
        let addr = "127.0.0.1:80".to_string();
        let settings = ConnectionSettings {
            connect_retry_initial_delay: Duration::new(0, 1),
            connect_retry_max_delay: Duration::new(0, 1),
            connect_retry_timeout: Duration::from_millis(100),
            ..Default::default()
        };
        Stream::<String, TestStream>::connect(addr, settings).unwrap();
    }

    #[test]
    fn connect_giveup() {
        let addr = "127.0.0.1".to_string();
        let settings = ConnectionSettings {
            connect_retry_initial_delay: Duration::from_millis(1),
            connect_retry_max_delay: Duration::from_millis(1),
            connect_retry_timeout: Duration::from_millis(5),
            ..Default::default()
        };
        let ret = Stream::<String, TestStream>::connect(addr, settings);
        assert_eq!(ret.err().unwrap().kind(), io::ErrorKind::ConnectionRefused);
    }

    #[test]
    fn reconnect() {
        let addr = "127.0.0.1:80".to_string();
        let settings = ConnectionSettings {
            connect_retry_initial_delay: Duration::new(0, 1),
            connect_retry_max_delay: Duration::new(0, 1),
            connect_retry_timeout: Duration::from_millis(100),
            ..Default::default()
        };
        let mut ret = Stream::<String, TestStream>::connect(addr, settings).unwrap();
        ret.reconnect().unwrap();
    }

    #[test]
    fn read_and_write() {
        use super::*;

        #[derive(Debug)]
        struct TS;
        lazy_static! {
            static ref QUEUE: Mutex<RefCell<VecDeque<Result<usize, io::Error>>>> = {
                let mut q = VecDeque::new();

                let scenario = vec![
                    Err(io::Error::from(io::ErrorKind::TimedOut)), // write ng.
                    Err(io::Error::from(io::ErrorKind::BrokenPipe)), // write ng then reconnect.
                    Ok(100), // write ok.
                    Err(io::Error::from(io::ErrorKind::WouldBlock)), // read ng.
                    Ok(64), // read ok.
                ];
                for s in scenario {
                    q.push_back(s);
                }
                Mutex::new(RefCell::new(q))
            };
        };

        impl Connect<TS> for TS {
            fn connect<A>(_addr: A, _s: ConnectionSettings) -> io::Result<TS>
            where
                A: ToSocketAddrs + Clone,
            {
                Ok(TS)
            }
        }
        impl Write for TS {
            fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
                let q = QUEUE.lock().unwrap();
                let mut q = q.borrow_mut();
                q.pop_front().unwrap()
            }
            fn flush(&mut self) -> io::Result<()> {
                let q = QUEUE.lock().unwrap();
                let mut q = q.borrow_mut();
                q.pop_front().unwrap().map(|_| ())
            }
        }
        impl Read for TS {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                let q = QUEUE.lock().unwrap();
                let mut q = q.borrow_mut();
                let a = q.pop_front().unwrap();
                if a.is_ok() {
                    let ack: [u8; 64] = [
                        0x81, 0xa3, 0x61, 0x63, 0x6b, 0xd9, 0x30, 0x5a, 0x6d, 0x46, 0x6c, 0x4e,
                        0x57, 0x5a, 0x6a, 0x4e, 0x6a, 0x45, 0x74, 0x59, 0x32, 0x55, 0x77, 0x5a,
                        0x43, 0x30, 0x30, 0x4e, 0x47, 0x45, 0x35, 0x4c, 0x57, 0x49, 0x31, 0x5a,
                        0x54, 0x4d, 0x74, 0x4d, 0x32, 0x59, 0x7a, 0x5a, 0x6a, 0x68, 0x69, 0x4e,
                        0x54, 0x51, 0x33, 0x5a, 0x6d, 0x45, 0x77, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00,
                    ];
                    for (i, b) in ack.iter().enumerate() {
                        buf[i] = *b;
                    }
                    a
                } else {
                    a
                }
            }
        }

        let settings = ConnectionSettings {
            write_retry_timeout: Duration::from_secs(30),
            write_retry_max_delay: Duration::from_secs(1),
            write_retry_initial_delay: Duration::from_millis(10),
            read_retry_timeout: Duration::from_secs(30),
            read_retry_max_delay: Duration::from_secs(1),
            read_retry_initial_delay: Duration::from_millis(10),
            ..Default::default()
        };

        let mut stream: Stream<String, TS> = Stream::connect("addr".to_string(), settings).unwrap();
        (&mut stream)
            .write_and_read(&[0x01], "ZmFlNWZjNjEtY2UwZC00NGE5LWI1ZTMtM2YzZjhiNTQ3ZmEw")
            .unwrap()
    }
}
