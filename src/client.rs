use crate::buffer::Buffer;
use crate::connect;
use crate::error::ClientError;
use crate::queue::RecoverySettings;
use crate::worker::{Message, Worker};
use crossbeam_channel::{bounded, unbounded, Sender};
use serde::Serialize;
use std::fmt::Debug;
use std::io;
use std::net::ToSocketAddrs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};

pub trait Client {
    fn send<A>(&self, tag: String, a: &A, timestamp: SystemTime) -> Result<(), ClientError>
    where
        A: Serialize;
    fn terminate(&self) -> Result<(), ClientError>;
}

pub struct WorkerPool {
    worker: Worker,
    sender: Sender<Message>,
    terminated: AtomicBool,
}

impl WorkerPool {
    pub fn create<A>(addr: &A) -> io::Result<Self>
    where
        A: ToSocketAddrs + Clone + Debug + Send + 'static,
    {
        Self::with_settings(addr, &Default::default())
    }
    pub fn with_settings<A>(addr: &A, settings: &Settings) -> io::Result<Self>
    where
        A: ToSocketAddrs + Clone + Debug + Send + 'static,
    {
        let (sender, receiver) = unbounded();

        info!("Worker creating...");

        let conn_settings = connect::ConnectionSettings {
            connect_retry_initial_delay: settings.connection_retry_initial_delay,
            connect_retry_max_delay: settings.connection_retry_max_delay,
            connect_retry_timeout: settings.connection_retry_timeout,
            write_timeout: settings.write_timeout,
            read_timeout: settings.read_timeout,
            write_retry_initial_delay: settings.write_retry_initial_delay,
            write_retry_max_delay: settings.write_retry_max_delay,
            write_retry_timeout: settings.write_retry_timeout,
            read_retry_initial_delay: settings.read_retry_initial_delay,
            read_retry_max_delay: settings.read_retry_max_delay,
            read_retry_timeout: settings.read_retry_timeout,
        };
        let worker = Worker::create(
            addr.clone(),
            conn_settings,
            receiver,
            settings.flush_period,
            settings.max_flush_entries,
            settings.recovery_settings,
        )?;

        Ok(Self {
            worker,
            sender,
            terminated: AtomicBool::new(false),
        })
    }
}

impl Client for WorkerPool {
    fn send<A>(&self, tag: String, a: &A, timestamp: SystemTime) -> Result<(), ClientError>
    where
        A: Serialize,
        A: Buffer<A>,
    {
        if self.terminated.load(Ordering::Acquire) {
            debug!("Worker does already closed.");
            return Ok(());
        }

        let buf = a.pack().map_err(ClientError::Buffer)?;

        self.sender
            .send(Message::Queuing(tag, timestamp, buf))
            .map_err(ClientError::SendChannel)?;
        Ok(())
    }

    fn terminate(&self) -> Result<(), ClientError> {
        if self.terminated.fetch_or(true, Ordering::SeqCst) {
            info!("Worker does already terminated.");
            return Ok(());
        }

        info!("Sending terminate message to worker.");

        let (sender, receiver) = bounded::<()>(0);
        self.sender.send(Message::Terminating(sender)).unwrap();
        receiver.recv().map_err(ClientError::RecieveChannel)?;

        Ok(())
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        self.terminate().unwrap();
        let wkr = &mut self.worker;

        info!("Shutting down worker.");

        wkr.join_handler();
    }
}

#[derive(Clone)]
pub struct Settings {
    pub flush_period: Duration,
    pub max_flush_entries: usize,
    pub connection_retry_initial_delay: Duration,
    pub connection_retry_max_delay: Duration,
    pub connection_retry_timeout: Duration,
    pub write_timeout: Duration,
    pub read_timeout: Duration,

    pub write_retry_initial_delay: Duration,
    pub write_retry_max_delay: Duration,
    pub write_retry_timeout: Duration,
    pub read_retry_initial_delay: Duration,
    pub read_retry_max_delay: Duration,
    pub read_retry_timeout: Duration,

    #[deprecated(
        since = "1.2.0",
        note = "use `recovery_settings` instead, it has no effect"
    )]
    pub does_recover: bool,
    pub recovery_settings: RecoverySettings,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            flush_period: Duration::from_millis(256),
            max_flush_entries: 1024,
            connection_retry_initial_delay: Duration::from_millis(50),
            connection_retry_max_delay: Duration::from_secs(5),
            connection_retry_timeout: Duration::from_secs(60),
            write_retry_initial_delay: Duration::from_millis(5),
            write_retry_max_delay: Duration::from_secs(5),
            write_retry_timeout: Duration::from_secs(30),
            read_retry_initial_delay: Duration::from_millis(5),
            read_retry_max_delay: Duration::from_secs(5),
            read_retry_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(1),
            read_timeout: Duration::from_secs(1),
            #[allow(deprecated)]
            does_recover: false,
            recovery_settings: RecoverySettings::default(),
        }
    }
}

#[cfg(test)]
mod test {}
