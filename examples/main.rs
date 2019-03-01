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
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant, SystemTime};

#[derive(Clone, Debug, PartialEq, Serialize)]
struct Human {
    age: u32,
    name: String,
}

lazy_static! {
    static ref POOL: Mutex<WorkerPool> = {
        let addr = "127.0.0.1:24224".to_string();
        let settins = Settings {
            workers: 1,
            flush_period: Duration::from_millis(10),
            max_flush_entries: 1000,
            connection_retry_timeout: Duration::from_secs(60),
            write_timeout: Duration::from_secs(30),
            read_timeout: Duration::from_secs(30),
            ..Default::default()
        };
        let pool =
            WorkerPool::with_settings(&addr, &settins).expect("Couldn't create the worker pool.");
        Mutex::new(pool)
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
            for _ in 0..10_000 {
                let name: String = rng.sample_iter(&Alphanumeric).take(30).collect();
                let age: u32 = rng.gen_range(1, 100);

                let tag = format!("test.human.age.{}", &age);
                let a = Human { age, name };
                let timestamp = SystemTime::now();

                let pool = POOL.lock().expect("Client couldn't be locked.");
                pool.send(tag, &a, timestamp).unwrap();

                let dur = rng.gen_range(10, 500000);
                thread::sleep(Duration::new(0, dur));
            }
        });
        calls.push(t);
    }

    for c in calls {
        c.join().expect("Couldn't join on the associated thread.");
    }

    info!("End sending messages.");

    let mut pool = POOL.lock().expect("Client couldn't be locked.");
    pool.close();

    info!("End. elapsed: {:?}", start.elapsed());
}
