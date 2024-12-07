use crate::{DataResult, DataStore, Equivalent};
use chrono::Utc;
use serde_json::Value;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Result, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};

/// Configuration options for the file-based data store.
///
/// This struct provides the configuration parameters needed to create a new DirectoryStore instance.
/// It controls where and how files are stored, including file naming, size limits, and organization.
///
/// # Examples
/// ```
/// use std::path::PathBuf;
/// use transientdb::DirectoryConfig;
///
/// let config = DirectoryConfig {
///     write_key: "my-store".into(),
///     storage_location: PathBuf::from("/tmp/data"),
///     base_filename: "events".into(),
///     max_file_size: 1024 * 1024, // 1MB
/// };
/// ```
pub struct DirectoryConfig {
	/// Key used to identify writes to this store.
	/// This is included in the metadata of each data file created by the store.
	pub write_key: String,
	/// Directory where data files will be stored.
	/// The store will create this directory if it doesn't exist.
	pub storage_location: PathBuf,
	/// Base name for generated files.
	/// Files will be created with the pattern: "{index}-{base_filename}"
	/// where index is an auto-incrementing number.
	pub base_filename: String,
	/// Maximum size in bytes for individual data files.
	/// Once a file reaches this size, a new file will be created.
	/// Must be at least 100 bytes.
	pub max_file_size: usize,
}

/// Type alias for the file validator function
pub type FileValidator = Box<dyn Fn(&Path) -> Result<()> + Send + Sync>;

/// A data store that persists items to files in a directory.
///
/// Files are created with incrementing numerical prefixes and contain batched JSON data.
/// When a file reaches the configured size limit, a new file is created.
/// Completed files are marked with a .temp extension to indicate they are ready for processing.
///
/// Each file contains a JSON object with:
/// - A `batch` array containing the stored items
/// - A `sentAt` timestamp in RFC3339 format
/// - The store's `writeKey`
pub struct DirectoryStore {
	config: DirectoryConfig,
	writer: Option<BufWriter<File>>,
	current_size: usize,
	current_path: Option<PathBuf>,
	file_validator: Option<FileValidator>,
	next_index: AtomicU32,
}

impl DirectoryStore {
	const TEMP_EXTENSION: &'static str = "temp";

	/// Creates a new DirectoryStore with the specified configuration.
	///
	/// Creates the storage directory if it doesn't exist. The store will initialize
	/// its file index by scanning existing files in the directory.
	///
	/// # Arguments
	/// * `config` - Configuration options for the store
	///
	/// # Errors
	/// Returns an IO error if:
	/// - The storage directory cannot be created
	/// - The directory cannot be read when scanning for existing files
	///
	/// # Panics
	/// * If max_file_size is less than 100 bytes
	pub fn new(config: DirectoryConfig) -> Result<Self> {
		if config.max_file_size < 100 {
			panic!("Seriously? max_file_size < 100 bytes? What exactly do you expect to store in there?");
		}

		fs::create_dir_all(&config.storage_location)?;

		// Initialize next_index by scanning existing files
		let mut max_index = 0;
		let files = fs::read_dir(&config.storage_location)?;
		for entry in files {
			let entry = entry?;
			if let Ok(file_name) = entry.file_name().into_string() {
				if let Some(index_str) = file_name.split('-').next() {
					if let Ok(index) = index_str.parse::<u32>() {
						max_index = max_index.max(index);
					}
				}
			}
		}

		Ok(DirectoryStore {
			config,
			writer: None,
			current_size: 0,
			current_path: None,
			file_validator: None,
			next_index: AtomicU32::new(max_index + 1),
		})
	}

	/// Sets a validator function that will be called before finalizing each data file.
	///
	/// The validator is called just before a file is marked as complete (renamed with .temp extension).
	/// If the validator returns an error, the file will not be finalized and the error will be propagated.
	///
	/// # Arguments
	/// * `validator` - Function that takes a file path and returns Ok(()) if the file is valid
	///
	/// # Examples
	/// ```
	/// use std::path::PathBuf;
	/// use std::fs;
	/// use std::io;
	/// use transientdb::{DirectoryConfig, DirectoryStore};
	///
	/// let mut store = DirectoryStore::new(DirectoryConfig {
	///     write_key: "test".into(),
	///     storage_location: PathBuf::from("/tmp/data"),
	///     base_filename: "events".into(),
	///     max_file_size: 1024,
	/// })?;
	///
	/// // Add a validator that checks file size and JSON validity
	/// store.set_file_validator(|path| {
	///     // Verify minimum file size
	///     let metadata = fs::metadata(path)?;
	///     if metadata.len() < 10 {
	///         return Err(io::Error::new(io::ErrorKind::Other, "File too small"));
	///     }
	///
	///     // Verify file contains valid JSON
	///     let content = fs::read_to_string(path)?;
	///     serde_json::from_str::<serde_json::Value>(&content)?;
	///
	///     Ok(())
	/// });
	/// # Ok::<(), std::io::Error>(())
	/// ```
	pub fn set_file_validator<F>(&mut self, validator: F)
	where
		F: Fn(&Path) -> Result<()> + 'static + Send + Sync,
	{
		self.file_validator = Some(Box::new(validator));
	}

	fn next_index(&self) -> u32 {
		self.next_index.fetch_add(1, Ordering::SeqCst)
	}

	fn start_file_if_needed(&mut self) -> Result<bool> {
		if self.writer.is_some() {
			return Ok(false);
		}

		let mut index = self.next_index();
		let mut attempts = 0;
		const MAX_ATTEMPTS: u32 = 1000; // Safeguard against infinite loops

		loop {
			let file_path = self
				.config
				.storage_location
				.join(format!("{}-{}", index, self.config.base_filename));

			match OpenOptions::new()
				.write(true)
				.create_new(true)
				.open(&file_path)
			{
				Ok(file) => {
					let mut writer = BufWriter::new(file);
					self.current_path = Some(file_path);

					if self.current_size == 0 {
						writer.write_all(b"{ \"batch\": [")?;
						self.current_size = "{ \"batch\": [".len();
						self.writer = Some(writer);
						return Ok(true);
					}

					self.writer = Some(writer);
					return Ok(false);
				}
				Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
					// File exists, try next index
					index = self.next_index();
					attempts += 1;

					if attempts >= MAX_ATTEMPTS {
						return Err(io::Error::new(
							io::ErrorKind::Other,
							"Failed to find available index after maximum attempts",
						));
					}
				}
				Err(e) => return Err(e), // Other errors are propagated
			}
		}
	}

	fn finish_file(&mut self) -> Result<()> {
		let _ = match self.writer.take() {
			Some(mut writer) => {
				// Changed writeln! to write! to avoid newline
				write!(
					writer,
					"],\"sentAt\":\"{}\",\"writeKey\":\"{}\"}}",
					Utc::now().to_rfc3339(),
					self.config.write_key
				)?;
				writer.flush()?;
				writer
			}
			None => return Ok(()),
		};

		// Use the stored path
		if let Some(current_path) = self.current_path.take() {
			// Run validation if configured
			if let Some(validator) = &self.file_validator {
				validator(&current_path)?;
			}

			// Rename to .temp to mark as complete
			let new_path = current_path.with_extension(Self::TEMP_EXTENSION);
			fs::rename(current_path, new_path)?;
		}

		self.current_size = 0;
		Ok(())
	}

	fn sorted_files(&self, include_unfinished: bool) -> Result<Vec<PathBuf>> {
		let mut files: Vec<PathBuf> = fs::read_dir(&self.config.storage_location)?
			.filter_map(Result::ok)
			.map(|e| e.path())
			.filter(|p| {
				if include_unfinished {
					true
				} else {
					p.extension().and_then(|ext| ext.to_str()) == Some(Self::TEMP_EXTENSION)
				}
			})
			.collect();

		files.sort_by(|a, b| {
			a.file_name()
				.unwrap_or_default()
				.cmp(b.file_name().unwrap_or_default())
		});
		Ok(files)
	}

	fn up_to_size(&self, max_bytes: usize, files: &[PathBuf]) -> Result<Vec<PathBuf>> {
		let mut result = Vec::new();
		let mut total_size: u64 = 0;

		for file in files {
			if let Ok(metadata) = fs::metadata(file) {
				let size = metadata.len();
				if total_size + size <= max_bytes as u64 {
					result.push(file.clone());
					total_size += size;
				} else {
					break;
				}
			}
		}

		Ok(result)
	}
}

impl DataStore for DirectoryStore {
	type Output = Vec<PathBuf>;

	fn has_data(&self) -> bool {
		fs::read_dir(&self.config.storage_location)
			.map(|entries| {
				entries.filter_map(Result::ok).any(|e| {
					e.path().extension().and_then(|ext| ext.to_str()) == Some(Self::TEMP_EXTENSION)
				})
			})
			.unwrap_or(false)
	}

	fn reset(&mut self) {
		if let Ok(files) = self.sorted_files(true) {
			let _ = self.remove(
				&files
					.iter()
					.map(|p| Box::new(p.clone()) as Box<dyn Equivalent>)
					.collect::<Vec<_>>(),
			);
		}
	}

	fn append(&mut self, data: Value) -> Result<()> {
		let started = self.start_file_if_needed()?;
		let writer = self
			.writer
			.as_mut()
			.ok_or_else(|| io::Error::new(io::ErrorKind::Other, "No active writer"))?;

		if self.current_size >= self.config.max_file_size {
			self.finish_file()?;
			return self.append(data);
		}

		if !started {
			writer.write_all(b",")?;
		}
		serde_json::to_writer(&mut *writer, &data)?;
		writer.flush()?;

		self.current_size += data.to_string().len();
		Ok(())
	}

	fn fetch(
		&mut self,
		count: Option<usize>,
		max_bytes: Option<usize>,
	) -> Result<Option<DataResult<Self::Output>>> {
		if self.writer.is_some() {
			self.finish_file()?;
		}

		let mut files = self.sorted_files(false)?;

		if let Some(max_bytes) = max_bytes {
			files = self.up_to_size(max_bytes, &files)?;
		}

		if let Some(count) = count {
			files.truncate(count);
		}

		if files.is_empty() {
			return Ok(None);
		}

		let removable = files
			.iter()
			.map(|p| Box::new(p.clone()) as Box<dyn Equivalent>)
			.collect::<Vec<_>>();

		Ok(Some(DataResult {
			data: Some(files),
			removable: Some(removable),
		}))
	}

	fn remove(&mut self, data: &[Box<dyn Equivalent>]) -> Result<()> {
		for item in data {
			if let Some(path) = item.as_any().downcast_ref::<PathBuf>() {
				if let Err(e) = fs::remove_file(path) {
					eprintln!("Failed to remove file {:?}: {}", path, e);
				}
			}
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::{DirectoryConfig, DirectoryStore};
	use crate::DataStore;
	use std::fs;
	use std::io;
	use std::io::Result;
	use tempfile::TempDir;

	#[test]
	fn test_directory_store_basic_operations() -> Result<()> {
		let temp_dir = TempDir::new()?;

		let config = DirectoryConfig {
			write_key: "test-key".to_string(),
			storage_location: temp_dir.path().to_owned(),
			base_filename: "events".to_string(),
			max_file_size: 1024,
		};

		let mut store = DirectoryStore::new(config)?;

		// Test append
		let event = serde_json::json!({
			"event": "test",
			"properties": {
				"value": 123
			}
		});
		store.append(event.clone())?;
		store.append(event.clone())?;

		// Force file completion
		if let Some(result) = store.fetch(None, None)? {
			assert!(result.data.is_some());
			let files = result.data.unwrap();
			assert_eq!(files.len(), 1);

			// Verify file content
			let content = fs::read_to_string(&files[0])?;
			assert!(content.contains("\"event\":\"test\""));
			assert!(content.contains("\"writeKey\":\"test-key\""));
		} else {
			panic!("Expected data but got none");
		}

		Ok(())
	}

	#[test]
	fn test_file_rotation() -> Result<()> {
		let temp_dir = TempDir::new()?;

		let config = DirectoryConfig {
			write_key: "test-key".to_string(),
			storage_location: temp_dir.path().to_owned(),
			base_filename: "events".to_string(),
			max_file_size: 100, // Small size to force rotation
		};

		let mut store = DirectoryStore::new(config)?;

		// Add multiple events that should span files
		for i in 0..5 {
			let event = serde_json::json!({
				"event": "test",
				"index": i,
				"data": "some longer data to help hit size limit...."
			});
			store.append(event)?;
		}

		// Fetch all files
		if let Some(result) = store.fetch(None, None)? {
			let files = result.data.unwrap();
			assert!(files.len() > 1, "Expected multiple files due to size limit");

			// Verify each file is properly formatted
			for file in &files {
				let content = fs::read_to_string(file)?.trim().to_string();
				assert!(content.starts_with("{ \"batch\": ["));
				assert!(content.ends_with("}"));
				assert!(content.contains("\"writeKey\":\"test-key\""));

				// Validate JSON structure
				let parsed: serde_json::Value = serde_json::from_str(&content)?;
				assert!(parsed.get("batch").is_some(), "Missing 'batch' field");
				assert!(parsed.get("writeKey").is_some(), "Missing 'writeKey' field");
				assert!(parsed.get("sentAt").is_some(), "Missing 'sentAt' field");
			}
		} else {
			panic!("Expected data but got none");
		}

		Ok(())
	}

	#[test]
	fn test_fetch_with_size_limit() -> Result<()> {
		let temp_dir = TempDir::new()?;
		let config = DirectoryConfig {
			write_key: "test-key".to_string(),
			storage_location: temp_dir.path().to_owned(),
			base_filename: "events".to_string(),
			max_file_size: 200,
		};

		let mut store = DirectoryStore::new(config)?;

		// Create several files of known size
		for i in 0..5 {
			let event = serde_json::json!({
				"event": "test",
				"index": i,
				"data": "padding data...."
			});
			store.append(event)?;
		}

		// Fetch with byte limit
		if let Some(result) = store.fetch(None, Some(250))? {
			let files = result.data.unwrap();
			let total_size: u64 = files.iter().map(|f| fs::metadata(f).unwrap().len()).sum();
			assert!(total_size <= 250, "Fetch exceeded size limit");
		}

		Ok(())
	}

	#[test]
	fn test_file_cleanup() -> Result<()> {
		let temp_dir = TempDir::new()?;
		let config = DirectoryConfig {
			write_key: "test-key".to_string(),
			storage_location: temp_dir.path().to_owned(),
			base_filename: "events".to_string(),
			max_file_size: 200,
		};

		let mut store = DirectoryStore::new(config)?;

		// Add some events
		let event = serde_json::json!({"event": "test"});
		store.append(event)?;

		// Fetch and then remove
		if let Some(result) = store.fetch(None, None)? {
			let files = result.data.unwrap();
			let removable = result.removable.unwrap();

			// Verify files exist
			for file in &files {
				assert!(file.exists());
			}

			// Remove the files
			store.remove(&removable)?;

			// Verify files are gone
			for file in &files {
				assert!(!file.exists());
			}
		}

		Ok(())
	}

	#[test]
	fn test_file_validator() -> Result<()> {
		let temp_dir = TempDir::new()?;
		let config = DirectoryConfig {
			write_key: "test-key".to_string(),
			storage_location: temp_dir.path().to_owned(),
			base_filename: "events".to_string(),
			max_file_size: 1024,
		};

		let mut store = DirectoryStore::new(config)?;

		// Set a validator that ensures files are larger than 10 bytes
		store.set_file_validator(|path| {
			let metadata = fs::metadata(path)?;
			if metadata.len() < 10 {
				return Err(io::Error::new(io::ErrorKind::Other, "File too small"));
			}
			Ok(())
		});

		let event = serde_json::json!({"event": "test"});
		store.append(event)?;

		// Fetch should trigger validation
		let result = store.fetch(None, None)?;
		assert!(result.is_some());

		Ok(())
	}

	#[test]
	fn test_fetch_limits() -> Result<()> {
		let temp_dir = TempDir::new()?;
		let config = DirectoryConfig {
			write_key: "test-key".to_string(),
			storage_location: temp_dir.path().to_owned(),
			base_filename: "events".to_string(),
			max_file_size: 100, // Small size to force multiple files
		};

		let mut store = DirectoryStore::new(config)?;

		// Create 10 files with predictable sizes
		for i in 0..10 {
			let event = serde_json::json!({
				"event": "test",
				"index": i,
				"data": "padding data to make files bigger..."
			});
			store.append(event)?;
		}

		// Test count limit only
		if let Some(result) = store.fetch(Some(3), None)? {
			let files = result.data.unwrap();
			assert_eq!(files.len(), 3, "Count limit not respected");
		}

		// Test byte limit only
		if let Some(result) = store.fetch(None, Some(250))? {
			let files = result.data.unwrap();
			let total_size: u64 = files.iter().map(|f| fs::metadata(f).unwrap().len()).sum();
			assert!(total_size <= 250, "Byte limit not respected");
		}

		// Test both count and byte limits
		if let Some(result) = store.fetch(Some(5), Some(200))? {
			let files = result.data.unwrap();
			assert!(
				files.len() <= 5,
				"Count limit not respected in combined test"
			);

			let total_size: u64 = files.iter().map(|f| fs::metadata(f).unwrap().len()).sum();
			assert!(
				total_size <= 200,
				"Byte limit not respected in combined test"
			);
		}

		Ok(())
	}

	#[test]
	#[should_panic(
		expected = "Seriously? max_file_size < 100 bytes? What exactly do you expect to store in there?"
	)]
	fn test_rejects_tiny_max_file_size() {
		let temp_dir = TempDir::new().unwrap();
		let config = DirectoryConfig {
			write_key: "test-key".to_string(),
			storage_location: temp_dir.path().to_owned(),
			base_filename: "events".to_string(),
			max_file_size: 50, // Try to use a ridiculously small size
		};

		// This should panic
		let _store = DirectoryStore::new(config).unwrap();
	}

	#[test]
	#[should_panic(
		expected = "Seriously? max_file_size < 100 bytes? What exactly do you expect to store in there?"
	)]
	fn test_rejects_zero_max_file_size() {
		let temp_dir = TempDir::new().unwrap();
		let config = DirectoryConfig {
			write_key: "test-key".to_string(),
			storage_location: temp_dir.path().to_owned(),
			base_filename: "events".to_string(),
			max_file_size: 0, // Try to use zero
		};

		// This should panic
		let _store = DirectoryStore::new(config).unwrap();
	}
}
