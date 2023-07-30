use std::{
    collections::HashMap,
    fs::File,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use crate::{handles::Handle, schedule::SlowInterval};
use parking_lot::RwLock;

const MINIMUM_FREE_HANDLES: usize = 2;
const MAXIMUM_HANDLE_SIZE: usize = 2000000000;

pub struct Collection {
    handles: Vec<Handle>,
    name: String,
    last_id: AtomicUsize,
    db_path: PathBuf,
}

impl Collection {
    pub fn new(
        name: String,
        db_path: PathBuf,
        handle_paths: Vec<(PathBuf, usize)>,
    ) -> Arc<RwLock<Collection>> {
        let collection = Arc::new(RwLock::new(Self {
            handles: handle_paths
                .iter()
                .map(|(p, id)| Handle::new(p.clone(), *id))
                .collect::<Vec<Handle>>(),
            name,
            last_id: AtomicUsize::new(0),
            db_path,
        }));

        let collection_clone = Arc::clone(&collection);
        let interval = SlowInterval::new(Duration::from_secs(10), move || {
            collection_clone.write().scan_handles();
        });

        interval.start().expect("Failed to spawn interval");
        collection
    }

    pub fn scan_handles(&mut self) {
        let mut handle_usage = HashMap::new();
        let mut free_handles = 0;

        for handle in &self.handles {
            let meta = handle
                .get_path()
                .metadata()
                .expect("Failed to read metadata for collection handle.");
            let size = meta.len() as f64;
            let usage: f64 = size / MAXIMUM_HANDLE_SIZE as f64 * 100.0;

            if usage < 100. {
                free_handles += 1;
            }

            handle_usage.insert(handle.get_id(), usage);
        }

        let last_id = self.handles.iter().map(|h| h.get_id()).max().unwrap_or(0);
        self.last_id.store(last_id, Ordering::SeqCst);

        if free_handles < MINIMUM_FREE_HANDLES {
            let needed = MINIMUM_FREE_HANDLES - free_handles;
            self.create_handles(needed, last_id);
        }

        println!(
            "Handle usages: {:#?}, free_handles: {}",
            handle_usage, free_handles
        );
    }

    fn create_handles(&mut self, needed: usize, last_id: usize) {
        for idx in 0..needed {
            let new_id = last_id + idx + 1;

            let mut path = self.db_path.clone();
            path.push(format!("collection-{}-{}.bin", self.name, new_id));

            File::create(&path).unwrap();

            self.handles.push(Handle::new(path, new_id));
        }
    }

    pub fn write(&self, bytes: Vec<u8>) {
        let free_handle = self
            .handles
            .iter()
            .find(|x| {
                let size = x.file_size();
                size < 2000000000 && !x.is_busy()
            })
            .or(Some(&self.handles[0]));

        if let Some(handle) = free_handle {
            let bytes = bytes;
            let handle = handle.clone();
            // rayon::spawn(move || {
            let data = bytes;
            handle.write(&data);
            // });
        }
    }
}
