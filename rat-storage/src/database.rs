use crate::collection::Collection;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::{create_dir, read_dir, DirEntry, File, OpenOptions},
    io::{Read, Write},
    path::PathBuf,
    sync::Arc,
};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("IO disk error")]
    Io(#[from] std::io::Error),

    #[error("Bincode error")]
    Bincode(#[from] bincode::Error),
}

#[derive(Serialize, Debug, Deserialize)]
pub struct DatabaseMetadata {
    name: String,
    collections: Vec<String>,
}

impl DatabaseMetadata {
    pub fn create(meta_path: PathBuf, db_name: String) -> Result<Self, DatabaseError> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(meta_path)?;
        let metadata = DatabaseMetadata {
            name: db_name,
            collections: Vec::new(),
        };

        let bytes = bincode::serialize(&metadata)?;
        file.write_all(&bytes)?;
        file.flush()?;
        Ok(metadata)
    }

    pub fn save_current(&self, meta_path: &PathBuf) -> Result<(), DatabaseError> {
        let bytes = bincode::serialize(&self)?;
        let mut file = OpenOptions::new()
            .write(true)
            .append(false)
            .open(meta_path)?;

        file.write_all(&bytes)?;
        file.flush()?;

        Ok(())
    }

    pub fn load(meta_path: PathBuf) -> Result<Self, DatabaseError> {
        let mut file = File::open(meta_path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).unwrap();

        Ok(bincode::deserialize(&buffer)?)
    }
}

pub struct Database {
    collections: HashMap<String, Arc<RwLock<Collection>>>,
    path: PathBuf,
    metadata: DatabaseMetadata,
    metadata_path: PathBuf,
}

impl Database {
    pub fn new(db_path: PathBuf) -> Result<Self, DatabaseError> {
        let mut metadata_path = db_path.clone();
        metadata_path.push("metadata.bin");

        let mut collection_map = HashMap::new();
        let metadata = DatabaseMetadata::load(metadata_path.clone())?;
        let items = read_dir(&db_path)?
            .map(|x| x.expect("Failed to read the database directory"))
            .collect::<Vec<DirEntry>>();

        for coll in &metadata.collections {
            let mut handles = Vec::new();
            let prefix = format!("collection-{}", coll);

            for item in items.iter() {
                let itm = item;
                let file_name = itm.file_name().to_str().unwrap().to_owned();

                if file_name.starts_with(&prefix) {
                    let id = file_name
                        .strip_prefix(&prefix)
                        .and_then(|x| x.strip_prefix('-'))
                        .and_then(|x| x.strip_suffix(".bin"))
                        .map(|x| x.parse::<usize>().unwrap())
                        .expect("Failed to parse collection handle id");

                    handles.push((itm.path(), id));
                }
            }

            let collection = Collection::new(coll.to_owned(), db_path.clone(), handles);
            collection_map.insert(coll.to_owned(), collection);
        }

        Ok(Self {
            collections: collection_map,
            path: db_path,
            metadata,
            metadata_path,
        })
    }

    pub fn create_collection(&mut self, collection_name: String) -> Result<(), DatabaseError> {
        if self.metadata.collections.contains(&collection_name) {
            return Ok(());
        }

        self.metadata.collections.push(collection_name.to_owned());
        self.metadata.save_current(&self.metadata_path)?;

        let collection = Collection::new(collection_name.clone(), self.path.clone(), vec![]);
        let mut lock = collection.write();
        lock.scan_handles();

        drop(lock);
        self.collections.insert(collection_name, collection);

        Ok(())
    }

    pub fn collection(&self, collection_name: &str) -> Option<Arc<RwLock<Collection>>> {
        self.collections
            .iter()
            .find(|x| x.0 == collection_name)
            .map(|x| x.1.clone())
    }

    pub fn create_new(db_name: String) -> Result<(), DatabaseError> {
        let path = PathBuf::from(format!("./data/{}", db_name));
        create_dir(&path)?;

        let mut metadata_path = path;
        metadata_path.push("metadata.bin");
        DatabaseMetadata::create(metadata_path.clone(), db_name)?;
        Ok(())
    }
}
