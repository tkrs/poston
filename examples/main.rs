#[macro_use]
extern crate lazy_static;
extern crate poston;
extern crate rand;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;
extern crate env_logger;

use rand::Rng;
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use poston::{Client, Settings, WorkerPool};

#[derive(Clone, Debug, PartialEq, Serialize)]
struct Human {
    age: u32,
    name: String,
}

lazy_static! {
    static ref POOL: Mutex<WorkerPool> = {
        let addr = "127.0.0.1:24224".to_string();
        // let pool = WorkerPool::new(&"127.0.0.1:24224".to_string()).expect("Couldn't create the worker pool.");
        let settins = Settings {
            workers: 4,
            flush_period: Duration::from_millis(64),
            max_flush_entries: 5120,
            ..Default::default()
        };
        let pool = WorkerPool::with_settings(&addr, &settins)
            .expect("Couldn't create the worker pool.");
        Mutex::new(pool)
    };
}

fn main() {
    env_logger::init();

    info!("Start.");

    let mut calls = Vec::new();

    let start = Instant::now();

    for i in 0..2 {
        let t = thread::spawn(move || {
            let mut rng = rand::thread_rng();
            for _ in 0..500_000 {
                let name = String::from("tkrs");
                let age: u32 =
                    if rng.gen() { rng.gen_range(0, 100) } else { i };

                let tag = format!("test.human.{}", i);
                let a = Human { age, name };
                let timestamp = SystemTime::now();

                let pool = POOL.lock().expect("Client couldn't be locked.");
                pool.send(tag, &a, timestamp).unwrap();
                thread::sleep(Duration::new(0, 5));
            }
        });
        calls.push(t);
    }

    for c in calls {
        c.join().expect("Couldn't join on the associated thread");
    }

    info!("End. elapsed: {:?}", start.elapsed());
}
