use parking_lot::RwLock;
use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

use crate::{doc::Document, reader::Reader};

#[derive(PartialEq, Clone)]
pub struct DataLocation(usize, usize);

#[derive(Clone)]
pub struct Handle {
    internal: Arc<HandleInternal>,
}

impl Handle {
    pub fn new(path: PathBuf, id: usize) -> Self {
        Self {
            internal: Arc::new(HandleInternal::new(path, id)),
        }
    }

    pub fn get_id(&self) -> usize {
        self.internal.id
    }

    pub fn get_path(&self) -> &PathBuf {
        &self.internal.path
    }

    pub fn is_busy(&self) -> bool {
        self.internal.is_busy.load(Ordering::SeqCst)
    }

    pub fn file_size(&self) -> usize {
        self.internal.file_size.load(Ordering::SeqCst)
    }

    pub fn read(&self) {
        let write_operation = self.internal.write_op.read().clone();
        let file = File::open(self.get_path()).unwrap();
        let mut buf_reader = BufReader::new(file);
        let mut reader: Reader<Document> = Reader::new();

        let mut bytes_read = 0usize;
        let mut need_to_cut = 0;
        let mut documents = Vec::with_capacity(64);

        loop {
            let mut buffer = buf_reader.fill_buf().unwrap().to_vec();
            let size = buffer.len();

            if buffer.is_empty() {
                break;
            }

            if need_to_cut > buffer.len() {
                need_to_cut -= buffer.len();
                continue;
            }

            if need_to_cut > 0 {
                buffer.splice(..need_to_cut, std::iter::empty());
            }
            if let Some(write) = &write_operation {
                let start_position = write.0 - bytes_read;
                if start_position <= size {
                    let end_position = write.1 - bytes_read;
                    if end_position <= size {
                        buffer.splice(start_position..end_position, std::iter::empty());
                    } else {
                        buffer.splice(start_position.., std::iter::empty());

                        let bytes_cut = size - start_position;
                        need_to_cut = end_position - bytes_cut;
                    }
                }
            }

            reader.read_bytes(&buffer);

            let read_docs: Vec<Document> = reader
                .collected_documents
                .drain(..)
                .collect::<Vec<Document>>();
            documents.extend(read_docs);

            bytes_read += size;
            buf_reader.consume(size);
        }

        println!("Read {} documents", documents.len());
    }

    pub fn write(&self, bytes: &[u8]) {
        self.internal.is_busy.store(true, Ordering::SeqCst);
        let mut write_op = self.internal.write_op.write();

        // once we locked we can update the file size
        let size = self.file_size();
        self.internal.file_size.store(size + bytes.len(), Ordering::SeqCst);
        *write_op = Some(DataLocation(size, size + bytes.len()));

        drop(write_op);

        let mut file = self.internal.write_handle.write();
        file.write_all(bytes).unwrap();
        file.flush().unwrap();

        drop(file);

        let mut write_op = self.internal.write_op.write();
        *write_op = None;

        self.internal.is_busy.store(false, Ordering::SeqCst);
    }
}

struct HandleInternal {
    write_handle: RwLock<BufWriter<File>>,
    write_op: RwLock<Option<DataLocation>>,
    file_size: AtomicUsize,
    is_busy: AtomicBool,
    path: PathBuf,
    id: usize,
}

impl HandleInternal {
    pub fn new(path: PathBuf, id: usize) -> Self {
        let wfile = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .open(&path)
            .expect("Failed to open the collection handle file");
        let file_length = wfile.metadata().unwrap().len() as usize;
        let write_handle = BufWriter::new(wfile);

        Self {
            write_handle: RwLock::new(write_handle),
            write_op: RwLock::new(Option::None),
            file_size: AtomicUsize::new(file_length),
            is_busy: AtomicBool::new(false),
            path,
            id,
        }
    }
}
