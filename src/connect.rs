use crate::buffer;
use crate::error::Error as MyError;
use backoff::{Error, ExponentialBackoff, Operation};
use std::cell::RefCell;
use std::fmt::Debug;
use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

pub trait Connect<T>
where
    T: TcpConfig,
{
    fn connect<A>(addr: A) -> io::Result<T>
    where
        A: ToSocketAddrs + Clone + Debug;
}

pub trait TcpConfig {
    fn set_nodelay(&self, v: bool) -> io::Result<()>;
    fn set_read_timeout(&self, v: Option<Duration>) -> io::Result<()>;
    fn set_write_timeout(&self, v: Option<Duration>) -> io::Result<()>;
}

pub trait Reconnect {
    fn reconnect(&mut self) -> io::Result<()>;
}

pub trait ConnectRetryDelay {
    fn connect_retry_initial_delay(&self) -> Duration;
    fn connect_retry_timeout(&self) -> Duration;
    fn connect_retry_max_delay(&self) -> Duration;
}

pub trait WriteRead {
    fn write_and_read(&mut self, buf: &[u8], chunk: &str) -> Result<(), Error<MyError>>;
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
}

impl<A, S> Stream<A, S>
where
    A: ToSocketAddrs + Clone + Debug,
    S: Connect<S> + TcpConfig,
{
    pub fn connect(addr: A, settings: ConnectionSettings) -> io::Result<Stream<A, S>> {
        let stream = connect_with_retry(addr.clone(), settings)?;
        let stream = RefCell::new(stream);
        Ok(Stream {
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
}

impl<A, S> Reconnect for Stream<A, S>
where
    A: ToSocketAddrs + Clone + Debug,
    S: Connect<S> + TcpConfig,
{
    fn reconnect(&mut self) -> io::Result<()> {
        let stream = connect_with_retry(self.addr.clone(), self.settings)?;
        *self.stream.borrow_mut() = stream;
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

impl<A, S> WriteRead for Stream<A, S>
where
    A: ToSocketAddrs + Clone + Debug,
    S: Connect<S> + TcpConfig + Read + Write,
{
    fn write_and_read(&mut self, buf: &[u8], chunk: &str) -> Result<(), Error<MyError>> {
        let mut backoff = ExponentialBackoff {
            current_interval: self.write_retry_initial_delay(),
            initial_interval: self.write_retry_initial_delay(),
            max_interval: self.write_retry_max_delay(),
            max_elapsed_time: Some(self.write_retry_timeout()),
            ..Default::default()
        };

        let mut op = || {
            if let Err(err) = self.write(buf) {
                warn!(
                    "Failed to write message, chunk: {}. cause: {:?}.",
                    chunk, err
                );
                let e = match err.kind() {
                    io::ErrorKind::BrokenPipe
                    | io::ErrorKind::NotConnected
                    | io::ErrorKind::ConnectionRefused
                    | io::ErrorKind::ConnectionAborted => self.reconnect(),
                    _ => Err(err),
                };
                return e.map_err(|e| Error::Transient(MyError::NetworkError(e)));
            }

            let mut resp_buf = [0u8; 64];
            let read_size = self.read(&mut resp_buf).map_err(|e| {
                warn!("Failed to read response, chunk: {}, cause: {:?}.", chunk, e);
                Error::Transient(MyError::NetworkError(e))
            })?;

            if read_size == 0 {
                warn!("Received empty response, chunk: {}.", chunk);
                Err(Error::Transient(MyError::NoAckResponseError))
            } else {
                let reply =
                    buffer::unpack_response(&resp_buf, read_size).map_err(Error::Transient)?;
                if reply.ack == chunk {
                    Ok(())
                } else {
                    warn!(
                        "Did not match ack and chunk, ack: {}, chunk: {}.",
                        reply.ack, chunk
                    );
                    if let Err(err) = self.reconnect() {
                        warn!("Failed to reconnect: {:?}.", err);
                    }
                    Err(Error::Transient(MyError::AckUmatchedError(
                        reply.ack,
                        chunk.to_string(),
                    )))
                }
            }
        };

        op.retry(&mut backoff)
    }
}

impl TcpConfig for TcpStream {
    fn set_nodelay(&self, v: bool) -> io::Result<()> {
        self.set_nodelay(v)
    }

    fn set_read_timeout(&self, v: Option<Duration>) -> io::Result<()> {
        self.set_read_timeout(v)
    }

    fn set_write_timeout(&self, v: Option<Duration>) -> io::Result<()> {
        self.set_write_timeout(v)
    }
}

impl Connect<TcpStream> for TcpStream {
    fn connect<A>(addr: A) -> io::Result<TcpStream>
    where
        A: ToSocketAddrs + Clone + Debug,
    {
        TcpStream::connect(addr)
    }
}

fn connect_with_retry<C, A>(addr: A, settings: ConnectionSettings) -> io::Result<C>
where
    A: ToSocketAddrs + Clone + Debug,
    C: Connect<C> + TcpConfig,
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
        debug!("Start connect to {:?}.", addr);
        C::connect(&addr)
            .map(|s| {
                s.set_nodelay(true).unwrap();
                s.set_read_timeout(Some(settings.read_timeout)).unwrap();
                s.set_write_timeout(Some(settings.write_timeout)).unwrap();
                s
            })
            .map_err(|err| {
                warn!("Failed to connect to {:?}.", addr);
                Error::Transient(err)
            })
    };

    op.retry(&mut backoff).map_err(|err| match err {
        Error::Transient(e) => e,
        Error::Permanent(e) => e,
    })
}

#[cfg(test)]
mod tests {
    use super::{io, Duration, ToSocketAddrs};
    use super::{Connect, ConnectionSettings, Reconnect, Stream, TcpConfig};

    mod s {
        use super::*;
        use std::convert::From;
        use std::io::{Read, Write};
        use std::sync::atomic::{AtomicUsize, Ordering};

        static CONN_COUNT: AtomicUsize = AtomicUsize::new(1);

        #[derive(Debug)]
        pub struct TestStream(AtomicUsize);
        impl Connect<TestStream> for TestStream {
            fn connect<A>(addr: A) -> io::Result<TestStream>
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
        impl TcpConfig for TestStream {
            fn set_nodelay(&self, _v: bool) -> io::Result<()> {
                Ok(())
            }
            fn set_read_timeout(&self, _v: Option<Duration>) -> io::Result<()> {
                Ok(())
            }
            fn set_write_timeout(&self, _v: Option<Duration>) -> io::Result<()> {
                Ok(())
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
        Stream::<String, s::TestStream>::connect(addr, settings).unwrap();
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
        let ret = Stream::<String, s::TestStream>::connect(addr, settings);
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
        let mut ret = Stream::<String, s::TestStream>::connect(addr, settings).unwrap();
        ret.reconnect().unwrap();
    }
}
