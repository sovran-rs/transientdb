mod directory;
mod memory;
mod transient;

use std::any::Any;
use std::fmt::Debug;
use std::path::PathBuf;

pub use directory::{DirectoryConfig, DirectoryStore};
pub use memory::{MemoryConfig, MemoryStore};
pub use transient::TransientDB;

/// Represents the result of a data fetch operation.
/// Contains either raw data bytes or paths to data files, along with items that can be removed.
#[derive(Debug)]
pub struct DataResult {
	/// Raw data bytes if using in-memory storage
	pub data: Option<Vec<u8>>,
	/// Paths to data files if using file-based storage
	pub data_files: Option<Vec<PathBuf>>,
	/// Items that can be removed after processing
	pub removable: Option<Vec<Box<dyn Equivalent>>>,
}

/// Indicates whether a store implementation uses in-memory data or files for storage.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DataTransactionType {
	/// Store operates on in-memory data
	Data,
	/// Store operates on files
	File,
}

/// Trait for types that can be compared for equality and downcasted.
/// Used primarily for tracking removable items in the data stores.
pub trait Equivalent: Any + Debug {
	/// Checks if this item equals another Equivalent item
	fn equals(&self, other: &dyn Equivalent) -> bool;

	/// Allows downcasting to concrete type
	fn as_any(&self) -> &dyn Any;
}

/// Core trait defining the interface for all data store implementations.
/// Provides methods for storing, retrieving and managing data.
pub trait DataStore {
	/// Returns true if the store contains any data
	fn has_data(&self) -> bool;
	/// Returns whether this store operates on in-memory data or files
	fn transaction_type(&self) -> DataTransactionType;
	/// Removes all data from the store
	fn reset(&mut self);
	/// Appends a new JSON value to the store
	///
	/// # Errors
	/// Returns an IO error if the append operation fails.
	fn append(&mut self, data: serde_json::Value) -> std::io::Result<()>;
	/// Fetches data from the store with optional limits on count and size.
	/// Returns None if no data is available.
	///
	/// # Arguments
	/// * `count` - Optional maximum number of items to fetch
	/// * `max_bytes` - Optional maximum total size in bytes to fetch
	fn fetch(
		&mut self,
		count: Option<usize>,
		max_bytes: Option<usize>,
	) -> std::io::Result<Option<DataResult>>;
	/// Removes the specified items from the store
	///
	/// # Arguments
	/// * `data` - Slice of boxed items implementing Equivalent to remove
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

	fn as_any(&self) -> &dyn Any {
		self
	}
}
