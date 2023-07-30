use database::Database;
use doc::Document;
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use schedule::IntervalManager;
use std::{collections::HashMap, path::PathBuf, time::Instant};

mod collection;
mod database;
mod doc;
mod handles;
mod reader;
mod schedule;

fn main() {
    tracing_subscriber::fmt::init();
    IntervalManager::init();

    let mut database = Database::new(PathBuf::from("./data/CoolDatabase/")).unwrap();
    database.create_collection("users".to_owned()).unwrap();

    let collection = database.collection("users").unwrap();
    let collection = collection.read();
    let start = Instant::now();
    (0..1000000)
        .into_par_iter()
        .map(|_| collection.write(Document::new(1, HashMap::new()).serialize()))
        .collect::<Vec<()>>();
    println!("Elapsed: {:.2?}", start.elapsed());
}
