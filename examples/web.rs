#[macro_use]
extern crate serde_derive;

use actix_web::web::{Data, Json, Path};
use actix_web::{error, get, middleware, App, HttpServer};
use log::info;
use poston::{Client, Settings, WorkerPool};
use rand::prelude::*;
use rand::{self, distr::Alphanumeric};
use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};
use std::{io, iter};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Member {
    age: u32,
    name: String,
}

#[get("/member/{id}")]
async fn member(
    path: Path<u32>,
    client: Data<WorkerPool>,
    db: Data<BTreeMap<u32, Member>>,
) -> actix_web::Result<Json<Member>> {
    let id = path.into_inner();
    let m = db
        .get(&id)
        .cloned()
        .ok_or_else(|| error::ErrorNotFound("Customer not found"))?;

    client
        .send("customer".into(), &m, SystemTime::now())
        .map_err(|e| error::ErrorInternalServerError(e.to_string()))?;

    Ok(Json(m))
}

fn init() {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "debug");
    }
    pretty_env_logger::init();
}

#[actix_web::main]
async fn main() -> io::Result<()> {
    init();

    info!("Start example.");

    let fluentd_addr = "127.0.0.1:24224";
    let fluentd_client = prepare_fluentd_client(fluentd_addr.into())
        .unwrap_or_else(|e| panic!("Cannot create the fluentd client: {}", e));
    let client = Data::new(fluentd_client);
    info!("Fluentd client prepared for {}.", fluentd_addr);

    let db = Data::new(prepare_db());
    info!("Database prepared with {} members.", db.len());

    let server_addr = ("127.0.0.1", 8080);
    info!(
        "Starting server at http://{}:{}",
        server_addr.0, server_addr.1
    );

    HttpServer::new(move || {
        App::new()
            .wrap(middleware::Logger::default())
            .app_data(client.clone())
            .app_data(db.clone())
            .service(member)
    })
    .bind(server_addr)?
    .workers(2)
    .run()
    .await?;

    info!("Bye ✌️");

    Ok(())
}

fn prepare_db() -> BTreeMap<u32, Member> {
    let mut db = BTreeMap::new();
    db.insert(
        1u32,
        Member {
            age: 30,
            name: "Alice".into(),
        },
    );
    db.insert(
        2u32,
        Member {
            age: 23,
            name: "Bob".into(),
        },
    );

    let mut rng = rand::rng();
    for i in 3..50_000 {
        let name: String = iter::repeat(())
            .map(|_| rng.sample(Alphanumeric))
            .map(char::from)
            .take(30)
            .collect();
        let age: u32 = rng.random_range(1..100);

        db.insert(i, Member { age, name });
    }
    db
}

fn prepare_fluentd_client(addr: String) -> Result<WorkerPool, io::Error> {
    let settins = Settings {
        flush_period: Duration::from_secs(3),
        max_flush_entries: 200,
        connection_retry_timeout: Duration::from_secs(5),
        write_timeout: Duration::from_secs(10),
        read_timeout: Duration::from_secs(10),
        does_recover: true,
        ..Default::default()
    };
    WorkerPool::with_settings(&addr, &settins)
}
