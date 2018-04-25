use std::cell::RefCell;
use std::io::{self, ErrorKind, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::thread;
use std::time::{Duration, Instant};

pub struct Stream<A: ToSocketAddrs + Clone> {
    addr: A,
    stream: RefCell<TcpStream>,
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

impl <A: ToSocketAddrs + Clone> Stream<A> {
    pub fn connect(addr: A, settings: ConnectionSettings) -> io::Result<Stream<A>>
    {
        let stream = connect(addr.clone(), settings.clone())?;
        let stream = RefCell::new(stream);
        Ok(Stream {
            addr,
            stream,
            settings,
        })
    }
}
pub trait Reconnect {
    fn reconnect(&mut self) -> io::Result<()>;
}

impl<A: ToSocketAddrs + Clone> Reconnect for Stream<A> {
    fn reconnect(&mut self) -> io::Result<()> {
        let stream = connect(self.addr.clone(), self.settings)?;
        *self.stream.borrow_mut() = stream;
        Ok(())
    }
}

impl<A: ToSocketAddrs + Clone> Write for Stream<A> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.stream.borrow_mut().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.stream.borrow_mut().flush()
    }
}

impl<A: ToSocketAddrs + Clone> Read for Stream<A> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.stream.borrow_mut().read(buf)
    }
}

fn connect<A>(addr: A, settings: ConnectionSettings) -> io::Result<TcpStream>
where
    A: ToSocketAddrs + Clone,
{
    let start = Instant::now();
    let mut retry_delay = settings.connect_retry_initial_delay;
    loop {
        let r = TcpStream::connect(addr.clone());
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

pub trait ReconnectableWriter {
    fn write(&mut self, buf: Vec<u8>) -> io::Result<()>;
}

impl<A: ToSocketAddrs + Clone> ReconnectableWriter for Stream<A> {
    fn write(&mut self, buf: Vec<u8>) -> io::Result<()> {
        let start = Instant::now();
        let mut retry_delay = self.settings.write_retry_initial_delay;
        loop {
            let r = self.write_all(&buf[..]);
            if Instant::now().duration_since(start) > self.settings.write_retry_timeout {
                return r;
            }
            retry_delay = if retry_delay >= self.settings.write_retry_max_delay {
                self.settings.write_retry_max_delay
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
                            match self.reconnect() {
                                Err(e) => {
                                    return Err(e);
                                }
                                _ => {}
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}
