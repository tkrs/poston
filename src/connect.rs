use std::cell::RefCell;
use std::io::{self, ErrorKind, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::thread;
use std::time::{Duration, Instant};

pub trait RetryableConnect<T>
where
    T: Connect<T>,
    T: Write + Read + TcpConfig,
{
    fn connect_with_retry<A>(addr: A, settings: ConnectionSettings) -> io::Result<T>
    where
        A: ToSocketAddrs + Clone;
}

pub trait Connect<T>
where
    T: TcpConfig,
{
    fn connect<A>(addr: A) -> io::Result<T>
    where
        A: ToSocketAddrs + Clone;
}

pub trait TcpConfig {
    fn set_nodelay(&self, v: bool) -> io::Result<()>;
    fn set_read_timeout(&self, v: Option<Duration>) -> io::Result<()>;
    fn set_write_timeout(&self, v: Option<Duration>) -> io::Result<()>;
}

pub trait Reconnect {
    fn reconnect(&mut self) -> io::Result<()>;
}

pub trait ReconnectWrite {
    fn write(&mut self, buf: Vec<u8>) -> io::Result<()>;
}

pub trait ConnectRetryDelay {
    fn now(&self) -> Instant;
    fn connect_retry_initial_delay(&self) -> Duration;
    fn connect_retry_timeout(&self) -> Duration;
    fn connect_retry_max_delay(&self) -> Duration;
}

pub trait WriteRetryDelay {
    fn now(&self) -> Instant;
    fn write_retry_initial_delay(&self) -> Duration;
    fn write_retry_timeout(&self) -> Duration;
    fn write_retry_max_delay(&self) -> Duration;
}

pub struct Stream<A, S>
where
    A: ToSocketAddrs + Clone,
    S: RetryableConnect<S>,
    S: Connect<S>,
    S: Write + Read + TcpConfig,
{
    addr: A,
    stream: RefCell<S>,
    settings: ConnectionSettings,
}

#[derive(Clone, Copy)]
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
    A: ToSocketAddrs + Clone,
    S: RetryableConnect<S>,
    S: Connect<S>,
    S: Write + Read + TcpConfig,
{
    pub fn connect(addr: A, settings: ConnectionSettings) -> io::Result<Stream<A, S>> {
        let stream = S::connect_with_retry(addr.clone(), settings)?;
        let stream = RefCell::new(stream);
        Ok(Stream {
            addr,
            stream,
            settings,
        })
    }
}

impl<A, S> Reconnect for Stream<A, S>
where
    A: ToSocketAddrs + Clone,
    S: RetryableConnect<S>,
    S: Connect<S>,
    S: Write + Read + TcpConfig,
{
    fn reconnect(&mut self) -> io::Result<()> {
        let stream = S::connect_with_retry(self.addr.clone(), self.settings)?;
        *self.stream.borrow_mut() = stream;
        Ok(())
    }
}

impl<A, S> Write for Stream<A, S>
where
    A: ToSocketAddrs + Clone,
    S: RetryableConnect<S>,
    S: Connect<S>,
    S: Write + Read + TcpConfig,
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
    A: ToSocketAddrs + Clone,
    S: RetryableConnect<S>,
    S: Connect<S>,
    S: Write + Read + TcpConfig,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.stream.borrow_mut().read(buf)
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
        A: ToSocketAddrs + Clone,
    {
        TcpStream::connect(addr)
    }
}

impl<C> RetryableConnect<C> for C
where
    C: Connect<C>,
    C: Write + Read + TcpConfig,
{
    fn connect_with_retry<A>(addr: A, settings: ConnectionSettings) -> io::Result<C>
    where
        A: ToSocketAddrs + Clone,
    {
        let start = Instant::now();
        let mut retry_delay = settings.connect_retry_initial_delay;
        loop {
            let r = C::connect(addr.clone());
            match r {
                Ok(s) => {
                    s.set_nodelay(true).unwrap();
                    s.set_read_timeout(Some(settings.read_timeout)).unwrap();
                    s.set_write_timeout(Some(settings.write_timeout)).unwrap();
                    return Ok(s);
                }
                e => {
                    if Instant::now().duration_since(start) > settings.connect_retry_timeout {
                        return e;
                    }
                    thread::sleep(retry_delay);
                    if retry_delay >= settings.connect_retry_max_delay {
                        retry_delay = settings.connect_retry_max_delay;
                    } else {
                        retry_delay = retry_delay + retry_delay;
                    }
                }
            }
        }
    }
}

impl<A, S> WriteRetryDelay for Stream<A, S>
where
    A: ToSocketAddrs + Clone,
    S: RetryableConnect<S>,
    S: Connect<S>,
    S: Write + Read + TcpConfig,
{
    fn now(&self) -> Instant {
        Instant::now()
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

impl<W: Write + Reconnect + WriteRetryDelay> ReconnectWrite for W {
    fn write(&mut self, buf: Vec<u8>) -> io::Result<()> {
        let start = self.now();
        let mut retry_delay = self.write_retry_initial_delay();
        loop {
            let r = self.write_all(&buf[..]);
            if Instant::now().duration_since(start) > self.write_retry_timeout() {
                return r;
            }
            retry_delay = if retry_delay >= self.write_retry_max_delay() {
                self.write_retry_max_delay()
            } else {
                retry_delay + retry_delay
            };
            match r {
                Ok(_) => return Ok(()),
                Err(e) => {
                    thread::sleep(retry_delay);
                    debug!("Write error found {:?}.", e);
                    match e.kind() {
                        ErrorKind::BrokenPipe
                        | ErrorKind::ConnectionRefused
                        | ErrorKind::ConnectionAborted => {
                            debug!("Try reconnect.");
                            if let Err(e) = self.reconnect() {
                                return Err(e);
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}
