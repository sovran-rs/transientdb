mod directory;
mod memory;
mod transient;

use std::any::Any;
use std::path::PathBuf;
use std::fmt::Debug;

#[derive(Debug)]
pub struct DataResult {
    pub data: Option<Vec<u8>>,
    pub data_files: Option<Vec<PathBuf>>,
    pub removable: Option<Vec<Box<dyn Equivalent>>>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DataTransactionType {
    Data,
    File,
}

pub trait Equivalent: Any + Debug {
    fn equals(&self, other: &dyn Equivalent) -> bool;

    // Make as_any a required method that returns &dyn Any
    fn as_any(&self) -> &dyn Any;
}

pub trait DataStore {
    fn has_data(&self) -> bool;
    fn count(&self) -> usize;
    fn transaction_type(&self) -> DataTransactionType;
    fn reset(&mut self);
    fn append(&mut self, data: serde_json::Value) -> std::io::Result<()>;
    fn fetch(&mut self, count: Option<usize>, max_bytes: Option<usize>) -> std::io::Result<Option<DataResult>>;
    fn remove(&mut self, data: &[Box<dyn Equivalent>]) -> std::io::Result<()>;
}

impl Equivalent for PathBuf {
    fn equals(&self, other: &dyn Equivalent) -> bool {
        if let Some(other_path) = other.as_any().downcast_ref::<PathBuf>() {
            self == other_path
        } else {
            false
        }
    }

    // Implement as_any for PathBuf
    fn as_any(&self) -> &dyn Any {
        self
    }
}