extern crate base64;
#[macro_use]
extern crate log;
extern crate rmp;
extern crate rmp_serde as rmps;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate uuid;

pub mod client;

mod buffer;
mod connect;
mod emitter;
mod error;
mod time_pack;
mod worker;

pub use client::{Client, WorkerPool, Settings};
