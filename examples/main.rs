#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

use poston::{Client, Settings, WorkerPool};
use pretty_env_logger;
use rand::prelude::*;
use rand::{self, distributions::Alphanumeric};
use std::time::{Duration, Instant, SystemTime};
use std::{iter, thread};

#[derive(Clone, Debug, PartialEq, Serialize)]
struct Human {
    age: u32,
    name: String,
}

lazy_static! {
    static ref POOL: WorkerPool = {
        let addr = "127.0.0.1:24224".to_string();
        let settins = Settings {
            flush_period: Duration::from_millis(10),
            max_flush_entries: 1000,
            connection_retry_timeout: Duration::from_secs(60),
            write_timeout: Duration::from_secs(30),
            read_timeout: Duration::from_secs(30),
            ..Default::default()
        };
        WorkerPool::with_settings(&addr, &settins).expect("Couldn't create the worker pool.")
    };
}

fn main() {
    pretty_env_logger::init();

    info!("Start.");

    let mut calls = Vec::new();

    let start = Instant::now();

    for i in 0..10 {
        let t = thread::spawn(move || {
            info!("Start sending messages. No {}.", i);
            let mut rng = thread_rng();
            for _ in 0..50_000 {
                let name: String = iter::repeat(())
                    .map(|_| rng.sample(Alphanumeric))
                    .map(char::from)
                    .take(30)
                    .collect();
                let age: u32 = rng.gen_range(1..100);

                let tag = format!("test.human.age.{}", &age);
                let a = Human { age, name };
                let timestamp = SystemTime::now();

                POOL.send(tag, &a, timestamp).unwrap();

                let dur = rng.gen_range(10..500000);
                thread::sleep(Duration::new(0, dur));
            }
        });
        calls.push(t);
    }

    for c in calls {
        c.join().expect("Couldn't join on the associated thread.");
    }

    info!("End sending messages.");

    POOL.terminate().unwrap();

    info!("End. elapsed: {:?}", start.elapsed());
}
