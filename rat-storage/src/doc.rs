use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::reader::Deserializable;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum DocumentValue {
    Integer(i32),
    Float(f64),
    String(String),
    Array(Vec<DocumentValue>),
    Object(HashMap<String, DocumentValue>),
}

impl DocumentValue {
    pub fn is_numeric(&self) -> bool {
        std::matches!(self, DocumentValue::Integer(_))
            || std::matches!(self, DocumentValue::Float(_))
    }
}

impl PartialEq for DocumentValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (DocumentValue::Integer(a), DocumentValue::Integer(b)) => a == b,
            (DocumentValue::Float(a), DocumentValue::Float(b)) => a == b,
            (DocumentValue::String(a), DocumentValue::String(b)) => a == b,
            (DocumentValue::Array(a), DocumentValue::Array(b)) => a == b,
            _ => false,
        }
    }
}

impl PartialOrd for DocumentValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(match (self.gt(other), self.lt(other)) {
            (true, _) => std::cmp::Ordering::Greater,
            (_, true) => std::cmp::Ordering::Less,
            _ => std::cmp::Ordering::Equal,
        })
    }

    fn gt(&self, other: &Self) -> bool {
        match (self, other) {
            (DocumentValue::Integer(a), DocumentValue::Integer(b)) => a > b,
            (DocumentValue::Float(a), DocumentValue::Float(b)) => a > b,
            (DocumentValue::String(a), DocumentValue::String(b)) => a > b,
            (DocumentValue::Array(a), DocumentValue::Array(b)) => a > b,
            _ => false,
        }
    }

    fn lt(&self, other: &Self) -> bool {
        match (self, other) {
            (DocumentValue::Integer(a), DocumentValue::Integer(b)) => a < b,
            (DocumentValue::Float(a), DocumentValue::Float(b)) => a < b,
            (DocumentValue::String(a), DocumentValue::String(b)) => a < b,
            (DocumentValue::Array(a), DocumentValue::Array(b)) => a < b,
            _ => false,
        }
    }

    fn le(&self, other: &Self) -> bool {
        self.lt(other) || self.eq(other)
    }

    fn ge(&self, other: &Self) -> bool {
        self.gt(other) || self.eq(other)
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
#[repr(u8)]
pub enum DocumentState {
    Deleted,
    DeletedUpdating,
    Exists,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Document {
    pub state: DocumentState,
    pub id: u64,
    pub fields: HashMap<String, DocumentValue>,
}

impl Document {
    pub fn new(id: u64, fields: HashMap<String, DocumentValue>) -> Document {
        Document {
            state: DocumentState::Exists,
            id,
            fields,
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut encoded: Vec<u8> = bincode::serialize(&self).unwrap();
        insert_vec_length(&mut encoded);

        encoded.push(0x0);
        encoded
    }
}

pub fn insert_vec_length(vec: &mut Vec<u8>) {
    let bytes = (vec.len() as u32).to_ne_bytes();
    vec.splice(0..0, bytes.iter().cloned());
}

impl Deserializable for Document {
    fn deserialize(value: &[u8]) -> bincode::Result<Self> {
        bincode::deserialize(value)
    }
}
