use tracing::debug;

pub fn vec_to_u32_ne(bytes: &[u8]) -> u32 {
    let mut result = [0u8; 4];
    result.copy_from_slice(bytes);
    u32::from_ne_bytes(result)
}

pub trait Deserializable {
    fn deserialize(value: &[u8]) -> bincode::Result<Self>
    where
        Self: std::marker::Sized;
}

pub struct Reader<T> {
    is_reading_document: bool,
    expecting_delimiter: bool,

    expected_bytes_count: u32,
    doc_bytes: Vec<u8>,
    doc_length_bytes: Vec<u8>,

    pub collected_documents: Vec<T>,
}

impl<T: Deserializable> Reader<T> {
    pub fn new() -> Reader<T> {
        Reader {
            is_reading_document: false,
            expecting_delimiter: false,
            expected_bytes_count: 0,
            doc_bytes: Vec::new(),
            doc_length_bytes: Vec::new(),
            collected_documents: Vec::new(),
        }
    }

    #[allow(dead_code)]
    pub fn read_bytes(&mut self, bytes: &[u8]) {
        self.collected_documents.clear();
        for byte in bytes {
            if !self.is_reading_document {
                // we're reading a new document
                self.doc_length_bytes.push(*byte);

                if self.doc_length_bytes.len() == 4 {
                    // we got the whole u32, we can now proceed to reading the document.
                    self.is_reading_document = true;
                    self.expected_bytes_count = vec_to_u32_ne(&self.doc_length_bytes);

                    self.doc_length_bytes.clear();
                    debug!(
                        "Got document length, EXPECTED_BYTES={}",
                        self.expected_bytes_count
                    );
                    continue;
                }

                debug!("Skip for now, size length={}", self.doc_length_bytes.len());
                continue;
            }

            if self.expecting_delimiter {
                if *byte != 0x0 {
                    panic!("Was expecting delimiter 0x0 but got: {}", byte);
                } else {
                    self.expecting_delimiter = false;
                    self.is_reading_document = false;
                    debug!("Consumed delimiter");
                }
                continue;
            }

            self.doc_bytes.push(*byte);

            if self.doc_bytes.len() == self.expected_bytes_count as usize {
                debug!(
                    "We've read the whole document, LENGTH={}",
                    self.doc_bytes.len()
                );

                self.collected_documents
                    .push(T::deserialize(&self.doc_bytes).unwrap());
                self.expecting_delimiter = true;
                self.doc_bytes.clear();
                continue;
            }
        }
    }

    #[allow(dead_code)]
    pub fn read_bytes_and_collect(&mut self, bytes: &[u8]) -> Vec<T> {
        self.read_bytes(bytes);
        self.collected_documents.drain(..).collect::<Vec<T>>()
    }
}
