#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

use rmp_serde as rmps;

pub mod client;

mod buffer;
mod connect;
mod emitter;
mod error;
mod time_pack;
mod worker;

pub use crate::client::{Client, Settings, WorkerPool};
