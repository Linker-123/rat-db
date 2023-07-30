use index::SingleIndexManager;
use std::path::PathBuf;

mod collection;
mod database;
mod doc;
mod handles;
mod index;
mod reader;
mod schedule;

fn main() {
    tracing_subscriber::fmt::init();
    // IntervalManager::init();

    // SingleIndexManager::create_new(
    //     PathBuf::from("./data/CoolDatabase/index-users-1.bin"),
    //     "users".to_owned(),
    //     "id".to_owned(),
    //     1,
    // )
    // .unwrap();

    let mut index_manager =
        SingleIndexManager::new(PathBuf::from("./data/CoolDatabase/index-users-1.bin")).unwrap();

    // index_manager
    //     .add_index(1, doc::DocumentValue::Integer(20))
    //     .unwrap();

    println!("{:#?}", index_manager.data);
}
