use crate::{
    doc::{insert_vec_length, DocumentValue},
    reader::{vec_to_u32_ne, Deserializable, Reader},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Read, Seek, Write},
    path::PathBuf,
};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum IndexError {
    #[error("Invalid sorting method")]
    Sorting,

    #[error("Bincode error")]
    Bincode(#[from] bincode::Error),

    #[error("I/O file system error")]
    Io(#[from] io::Error),

    #[error("The index metadata size is about 1024 bytes")]
    MetaTooBig,

    #[error("A file for this path already exsts")]
    PathExists,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct IndexMetaData {
    field: String,
    collection_name: String,
    sorting: i8,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Index {
    pub id: usize,
    pub value: DocumentValue,
}

impl Deserializable for Index {
    fn deserialize(value: &[u8]) -> bincode::Result<Self>
    where
        Self: std::marker::Sized,
    {
        bincode::deserialize(value)
    }
}

#[derive(Debug)]
pub struct SingleIndexManager {
    pub data: BTreeMap<DocumentValue, u64>,
    path: PathBuf,
    writer: BufWriter<File>,
    pub metadata: IndexMetaData,
}

impl SingleIndexManager {
    pub fn new(index_path: PathBuf) -> Result<SingleIndexManager, IndexError> {
        let file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(&index_path)?;
        let writer = BufWriter::new(file);

        // the first 1024 bytes are reserved for the index information
        let metadata = Self::read_metadata(&index_path)?;
        let mut indexes = Self {
            metadata,
            writer,
            path: index_path,
            data: BTreeMap::new(),
        };

        indexes.load_indexes()?;

        Ok(indexes)
    }

    fn read_metadata(index_path: &PathBuf) -> Result<IndexMetaData, IndexError> {
        let mut file = File::open(index_path)?;
        let mut buf = [0u8; 1024];

        file.read_exact(&mut buf)?;

        let mut buf = buf.to_vec();
        let length = buf.drain(0..4).collect::<Vec<u8>>();
        let length = vec_to_u32_ne(&length) as usize;

        let meta_bytes = buf.drain(..length).collect::<Vec<u8>>();
        let metadata: IndexMetaData = bincode::deserialize(&meta_bytes)?;

        Ok(metadata)
    }

    pub fn create_new(
        index_path: PathBuf,
        collection_name: String,
        field_name: String,
        sorting: i8,
    ) -> Result<(), IndexError> {
        if index_path.exists() {
            return Err(IndexError::PathExists);
        }

        if sorting != 1 && sorting != -1 {
            return Err(IndexError::Sorting);
        }

        let metadata = IndexMetaData {
            collection_name,
            field: field_name,
            sorting,
        };

        let mut bytes = bincode::serialize(&metadata)?;
        insert_vec_length(&mut bytes);

        if bytes.len() > 1024 {
            return Err(IndexError::MetaTooBig);
        }

        bytes.resize(1024, 0);
        assert!(bytes.len() == 1024);

        let mut file = File::create(index_path)?;
        file.write_all(&bytes)?;
        file.flush()?;

        Ok(())
    }

    pub fn load_indexes(&mut self) -> Result<(), IndexError> {
        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);
        let mut index_reader: Reader<Index> = Reader::new();

        reader.seek(io::SeekFrom::Start(1024))?;

        loop {
            let buffer = reader.fill_buf()?;
            let size = buffer.len();
            if buffer.is_empty() {
                break;
            }

            index_reader.read_bytes(buffer);
            reader.consume(size);

            for document in index_reader.collected_documents.drain(..) {
                self.data.insert(document.value, document.id as u64);
            }
        }
        Ok(())
    }

    pub fn add_index(&mut self, id: usize, value: DocumentValue) -> Result<(), IndexError> {
        let index = Index {
            id,
            value: value.clone(),
        };
        let mut bytes = bincode::serialize(&index)?;
        insert_vec_length(&mut bytes);

        let n = self.writer.write(&bytes)?;

        if n < bytes.len() {
            self.writer.flush()?;
        }

        self.data.insert(value, id as u64);

        Ok(())
    }
}
