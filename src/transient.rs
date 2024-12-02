use crate::{DataResult, DataStore, DataTransactionType, Equivalent};
use serde_json::Value;
use std::io;
use std::sync::Mutex;

/// A thread-safe wrapper around a data store implementation.
///
/// TransientDB provides synchronized access to any data store that implements the DataStore trait.
/// It uses interior mutability via a Mutex to allow multiple threads to safely interact with
/// the underlying store.
///
/// # Examples
/// ```
/// use transientdb::{MemoryConfig, MemoryStore, TransientDB};
/// use serde_json::json;
///
/// // Create a store instance
/// let store = MemoryStore::new(MemoryConfig {
///     write_key: "test".into(),
///     max_items: 1000,
///     max_fetch_size: 1024,
/// });
///
/// // Wrap it in TransientDB
/// let db = TransientDB::new(store);
///
/// // Multiple threads can now safely access the store
/// db.append(json!({"event": "test"}))?;
/// if let Some(result) = db.fetch(None, None)? {
///     // Process the data
/// }
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct TransientDB {
	store: Mutex<Box<dyn DataStore + Send>>,
}

impl TransientDB {
	/// Creates a new TransientDB instance wrapping the provided store.
	///
	/// # Arguments
	/// * `store` - Any implementation of DataStore that is Send + 'static
	///
	/// # Examples
	/// ```
	/// use transientdb::{DirectoryConfig, DirectoryStore, TransientDB};
	/// use std::path::PathBuf;
	///
	/// let store = DirectoryStore::new(DirectoryConfig {
	///     write_key: "test".into(),
	///     storage_location: PathBuf::from("/tmp/data"),
	///     base_filename: "events".into(),
	///     max_file_size: 1024,
	/// })?;
	///
	/// let db = TransientDB::new(store);
	/// # Ok::<(), std::io::Error>(())
	/// ```
	pub fn new(store: impl DataStore + Send + 'static) -> Self {
		Self {
			store: Mutex::new(Box::new(store)),
		}
	}

	/// Checks if the underlying store contains any data.
	pub fn has_data(&self) -> bool {
		self.store.lock().unwrap().has_data()
	}

	/// Returns the transaction type of the underlying store.
	pub fn transaction_type(&self) -> DataTransactionType {
		self.store.lock().unwrap().transaction_type()
	}

	/// Removes all data from the underlying store.
	pub fn reset(&self) {
		let mut store = self.store.lock().unwrap();
		store.reset();
	}

	/// Appends a JSON value to the underlying store.
	///
	/// # Arguments
	/// * `data` - The JSON value to append
	///
	/// # Errors
	/// Returns any IO error from the underlying store.
	pub fn append(&self, data: Value) -> io::Result<()> {
		let mut store = self.store.lock().unwrap();
		store.append(data)
	}

	/// Fetches data from the underlying store with optional limits.
	///
	/// # Arguments
	/// * `count` - Optional maximum number of items/files to fetch
	/// * `max_bytes` - Optional maximum total size in bytes to fetch
	///
	/// # Returns
	/// * `Ok(Some(DataResult))` if data was available
	/// * `Ok(None)` if no data was available
	/// * `Err` if there was an IO error
	pub fn fetch(
		&self,
		count: Option<usize>,
		max_bytes: Option<usize>,
	) -> io::Result<Option<DataResult>> {
		let mut store = self.store.lock().unwrap();
		store.fetch(count, max_bytes)
	}

	/// Removes items from the underlying store.
	///
	/// # Arguments
	/// * `data` - Slice of items to remove
	///
	/// # Errors
	/// Returns any IO error from the underlying store.
	pub fn remove(&self, data: &[Box<dyn Equivalent>]) -> io::Result<()> {
		let mut store = self.store.lock().unwrap();
		store.remove(data)
	}
}

#[cfg(test)]
mod tests {
	use crate::directory::{DirectoryConfig, DirectoryStore};
	use crate::memory::{MemoryConfig, MemoryStore};
	use crate::transient::TransientDB;
	use crate::DataTransactionType;
	use rand::Rng;
	use serde_json::{json, Value};
	use std::collections::HashSet;
	use std::io::Write;
	use std::sync::atomic::{AtomicUsize, Ordering};
	use std::sync::Arc;
	use std::time::Duration;
	use std::{fs, io, thread};
	use tempfile::TempDir;

	#[test]
	fn test_concurrent_appends() -> io::Result<()> {
		let config = MemoryConfig {
			write_key: "test-key-concurrent-appends".to_string(), // Unique key
			max_items: 2000,
			max_fetch_size: 1024,
		};
		let store = MemoryStore::new(config);
		let db = Arc::new(TransientDB::new(store));
		let mut handles = vec![];

		// Create a clone for final verification
		let final_db = db.clone();

		// Spawn 10 threads that each append 100 items
		for t in 0..10 {
			let db = db.clone();
			let handle = thread::spawn(move || {
				for i in 0..100 {
					let event = json!({
						"thread": t,
						"index": i,
						"data": "test data"
					});
					db.append(event).unwrap();
					// Add a tiny sleep to reduce contention
					if i % 10 == 0 {
						thread::sleep(std::time::Duration::from_micros(1));
					}
				}
			});
			handles.push(handle);
		}

		// Wait for all threads to complete
		for handle in handles {
			handle.join().unwrap();
		}

		// We should have some items, but maybe not all due to FIFO limits
		assert!(final_db.has_data(), "Should have at least some items");

		// Fetch and verify we have items from different threads
		let mut thread_counts = vec![0; 10];
		if let Some(result) = final_db.fetch(None, None)? {
			let batch: Value = serde_json::from_slice(result.data.as_ref().unwrap())?;
			let items = batch["batch"].as_array().unwrap();

			for item in items {
				let thread_id = item["thread"].as_i64().unwrap() as usize;
				thread_counts[thread_id] += 1;
			}

			// Verify we got items from multiple threads
			let threads_with_data = thread_counts.iter().filter(|&&c| c > 0).count();
			assert!(threads_with_data > 1, "Expected data from multiple threads");
		}

		Ok(())
	}

	#[test]
	fn test_concurrent_append_and_fetch() -> io::Result<()> {
		let config = MemoryConfig {
			write_key: "test-key-append-and-fetch".to_string(), // Unique key
			max_items: 1000,
			max_fetch_size: 1024,
		};

		let store = MemoryStore::new(config);
		let db = Arc::new(TransientDB::new(store));

		// Spawn thread for continuous appending
		let db_append = db.clone();
		let append_handle = thread::spawn(move || {
			for i in 0..500 {
				let event = json!({"index": i});
				db_append.append(event).unwrap();
				thread::sleep(std::time::Duration::from_micros(10));
			}
		});

		// Spawn thread for continuous fetching
		let db_fetch = db.clone();
		let fetch_handle = thread::spawn(move || {
			let mut total_fetched = 0;
			while total_fetched < 100 {
				if let Ok(Some(result)) = db_fetch.fetch(Some(10), None) {
					let batch: Value =
						serde_json::from_slice(result.data.as_ref().unwrap()).unwrap();
					let items = batch["batch"].as_array().unwrap();
					total_fetched += items.len();
				}
				thread::sleep(std::time::Duration::from_millis(1));
			}
			total_fetched
		});

		// Wait for both operations to complete
		append_handle.join().unwrap();
		let total_fetched = fetch_handle.join().unwrap();
		assert!(
			total_fetched >= 100,
			"Should have fetched at least 100 items"
		);

		Ok(())
	}

	#[test]
	fn test_concurrent_reset() -> io::Result<()> {
		let config = MemoryConfig {
			write_key: "test-key-reset".to_string(), // Unique key
			max_items: 1000,
			max_fetch_size: 1024,
		};
		let store = MemoryStore::new(config);
		let db = Arc::new(TransientDB::new(store));

		// Fill with some initial data
		for i in 0..100 {
			db.append(json!({"index": i}))?;
		}

		// Spawn threads for concurrent operations
		let mut handles = vec![];

		// Thread that resets
		let db_reset = db.clone();
		handles.push(thread::spawn(move || {
			thread::sleep(std::time::Duration::from_millis(50));
			db_reset.reset();
		}));

		// Thread that appends
		let db_append = db.clone();
		handles.push(thread::spawn(move || {
			for i in 0..100 {
				let event = json!({"new_index": i});
				let _ = db_append.append(event);
				thread::sleep(std::time::Duration::from_micros(100));
			}
		}));

		// Thread that fetches
		let db_fetch = db.clone();
		handles.push(thread::spawn(move || {
			let mut _fetch_count = 0;
			for _ in 0..10 {
				if let Ok(Some(result)) = db_fetch.fetch(None, None) {
					let batch: Value =
						serde_json::from_slice(result.data.as_ref().unwrap()).unwrap();
					let items = batch["batch"].as_array().unwrap();
					_fetch_count += items.len();
				}
				thread::sleep(std::time::Duration::from_millis(10));
			}
		}));

		// Wait for all operations to complete
		for handle in handles {
			handle.join().unwrap();
		}

		// The final state should be consistent (no panics or deadlocks)
		// everything has been handled, so there's no data.
		assert!(!db.has_data());

		Ok(())
	}

	#[test]
	fn test_concurrent_directory_store() -> io::Result<()> {
		let temp_dir = TempDir::new()?;
		let config = DirectoryConfig {
			write_key: "test-key-directory".to_string(),
			storage_location: temp_dir.path().to_owned(),
			base_filename: "events".to_string(),
			max_file_size: 200, // Small size to force rotation
		};

		let store = DirectoryStore::new(config)?;
		let db = Arc::new(TransientDB::new(store));

		// Spawn multiple threads for concurrent operations
		let mut handles = vec![];

		// Thread for appending
		let db_append = db.clone();
		handles.push(thread::spawn(move || {
			for i in 0..50 {
				let event = json!({
					"index": i,
					"data": "some test data to force file rotation"
				});
				db_append.append(event).unwrap();
				thread::sleep(std::time::Duration::from_millis(1));
			}
		}));

		// Thread for fetching
		let db_fetch = db.clone();
		handles.push(thread::spawn(move || {
			let mut _total_files = 0;
			for _ in 0..5 {
				if let Ok(Some(result)) = db_fetch.fetch(None, None) {
					if let Some(files) = result.data_files {
						_total_files += files.len();
					}
				}
				thread::sleep(std::time::Duration::from_millis(10));
			}
		}));

		// Thread for removing files
		let db_remove = db.clone();
		handles.push(thread::spawn(move || {
			for _ in 0..5 {
				if let Ok(Some(result)) = db_remove.fetch(Some(2), None) {
					if let Some(removable) = result.removable {
						db_remove.remove(&removable).unwrap();
					}
				}
				thread::sleep(std::time::Duration::from_millis(10));
			}
		}));

		// Wait for all operations to complete
		for handle in handles {
			handle.join().unwrap();
		}

		// Verify final state
		let final_files = std::fs::read_dir(temp_dir.path())?
			.filter_map(Result::ok)
			.count();

		// We should have some files but not all (due to removal)
		assert!(final_files < 50, "Some files should have been removed");
		assert!(final_files > 0, "Should still have some files");

		Ok(())
	}

	#[test]
	fn test_heavy_concurrent_load() -> io::Result<()> {
		let config = MemoryConfig {
			write_key: "test-key".to_string(),
			max_items: 1_000_000,        // 1M items max
			max_fetch_size: 1024 * 1024, // 1MB fetch size
		};
		let store = MemoryStore::new(config);
		let db = Arc::new(TransientDB::new(store));
		let mut handles = vec![];

		// Spawn 50 append threads, each writing 10K events
		for t in 0..50 {
			let db = db.clone();
			handles.push(thread::spawn(move || {
				for i in 0..10_000 {
					let event = json!({
						"thread": t,
						"index": i,
						"timestamp": chrono::Utc::now().to_rfc3339(),
						"data": "some test data that takes up space and makes the json bigger"
					});
					db.append(event).unwrap();
				}
			}));
		}

		// Spawn 20 aggressive fetch threads
		for _ in 0..20 {
			let db = db.clone();
			handles.push(thread::spawn(move || {
				let mut total_items = 0;
				loop {
					if let Ok(Some(result)) = db.fetch(Some(1000), None) {
						let batch: Value =
							serde_json::from_slice(result.data.as_ref().unwrap()).unwrap();
						let items = batch["batch"].as_array().unwrap();
						total_items += items.len();
						if total_items > 10_000 {
							break;
						}
					}
					thread::sleep(std::time::Duration::from_micros(100));
				}
			}));
		}

		// Spawn 5 reset threads that periodically try to reset
		for _ in 0..5 {
			let db = db.clone();
			handles.push(thread::spawn(move || {
				for _ in 0..10 {
					thread::sleep(std::time::Duration::from_millis(100));
					db.reset();
				}
			}));
		}

		// Wait for all operations to complete
		for handle in handles {
			handle.join().unwrap();
		}

		Ok(())
	}

	#[test]
	fn test_directory_store_stress() -> io::Result<()> {
		let temp_dir = TempDir::new()?;
		let config = DirectoryConfig {
			write_key: "test-key".to_string(),
			storage_location: temp_dir.path().to_owned(),
			base_filename: "events".to_string(),
			max_file_size: 1024 * 10, // 10KB files to force lots of rotation
		};

		let store = DirectoryStore::new(config)?;
		let db = Arc::new(TransientDB::new(store));
		let mut handles = vec![];

		// Spawn 30 append threads
		for t in 0..30 {
			let db = db.clone();
			handles.push(thread::spawn(move || {
				for i in 0..1000 {
					let event = json!({
						"thread": t,
						"index": i,
						"timestamp": chrono::Utc::now().to_rfc3339(),
						"data": "padding data to force file rotations frequently..."
					});
					db.append(event).unwrap();
					if i % 100 == 0 {
						thread::sleep(std::time::Duration::from_micros(100));
					}
				}
			}));
		}

		// Spawn 10 aggressive fetch/remove threads
		for _ in 0..10 {
			let db = db.clone();
			handles.push(thread::spawn(move || {
				for _ in 0..50 {
					if let Ok(Some(result)) = db.fetch(Some(5), None) {
						if let Some(removable) = result.removable {
							db.remove(&removable).unwrap();
						}
					}
					thread::sleep(std::time::Duration::from_millis(10));
				}
			}));
		}

		// Single thread doing periodic full fetches
		let db_fetch = db.clone();
		handles.push(thread::spawn(move || {
			for _ in 0..10 {
				if let Ok(Some(result)) = db_fetch.fetch(None, Some(1024 * 1024)) {
					if let Some(removable) = result.removable {
						db_fetch.remove(&removable).unwrap();
					}
				}
				thread::sleep(std::time::Duration::from_millis(100));
			}
		}));

		// Wait for all operations to complete
		for handle in handles {
			handle.join().unwrap();
		}

		// Try one final operation of each type
		db.append(json!({"final": "test"}))?;
		if let Some(result) = db.fetch(None, None)? {
			if let Some(removable) = result.removable {
				db.remove(&removable)?;
			}
		}

		Ok(())
	}

	#[test]
	fn test_memory_store_chaos() -> io::Result<()> {
		let config = MemoryConfig {
			write_key: "test-key".to_string(),
			max_items: 100_000,
			max_fetch_size: 1024 * 1024,
		};

		let store = MemoryStore::new(config);
		let db = Arc::new(TransientDB::new(store));
		let mut handles = vec![];

		assert_eq!(db.transaction_type(), DataTransactionType::Data);

		// Create a clone for final verification
		let final_db = db.clone();

		// Evil JSON generator (remains unchanged)
		let generate_evil_json = |i: u64| -> Value {
			match i % 5 {
				0 => json!({
					"normal": "boring",
					"timestamp": chrono::Utc::now().to_rfc3339(),
				}),
				1 => json!({
					"nested": {
						"deeply": {
							"nested": {
								"value": "here",
								"with": ["arrays", "of", "doom", "that", "go", "on", "forever"]
							}
						}
					}
				}),
				2 => json!({
					"unicode": "🦀💥👾👿",
					"weird_spaces": "    \n\t\r    ",
					"empty": "",
				}),
				3 => {
					let mut huge = json!({});
					for n in 0..100 {
						huge[format!("field_{}", n)] = json!("value");
					}
					huge
				}
				_ => {
					let mut tiny = json!({});
					tiny["x"] = json!("y");
					tiny
				}
			}
		};

		let random_delay = || {
			let mut rng = rand::thread_rng();
			if rng.gen_bool(0.2) {
				thread::sleep(std::time::Duration::from_millis(rng.gen_range(0..50)));
			}
		};

		// Spawn heavy append threads
		for _t in 0..30 {
			let db = db.clone();
			handles.push(thread::spawn(move || {
				let mut rng = rand::thread_rng();
				for i in 0..1000 {
					let evil_json = generate_evil_json(i as u64);
					random_delay();
					let _ = db.append(evil_json);

					if i % 100 == 0 {
						let _ = db.fetch(Some(rng.gen_range(0..10)), None);
					}
				}
			}));
		}

		// Spawn chaos threads
		for _ in 0..15 {
			let db = db.clone();
			handles.push(thread::spawn(move || {
				let mut rng = rand::thread_rng();
				for _ in 0..200 {
					match rng.gen_range(0..3) {
						0 => {
							db.reset();
						}
						1 => {
							// Random sized fetches
							let count = Some(rng.gen_range(0..1000));
							let bytes = Some(rng.gen_range(0..(1024 * 100)));
							let _ = db.fetch(count, bytes);
						}
						_ => {
							// Rapid-fire tiny appends
							for _ in 0..50 {
								let tiny_json = json!({"x": "y"});
								let _ = db.append(tiny_json);
							}
						}
					}
					random_delay();
				}
			}));
		}

		for handle in handles {
			handle.join().unwrap();
		}

		// Use final_db for a final operation to verify functionality
		final_db.append(json!({"final": "test"}))?;
		assert!(
			final_db.has_data(),
			"Store should have data after final append"
		);

		Ok(())
	}

	#[test]
	fn test_directory_store_chaos() -> io::Result<()> {
		let temp_dir = TempDir::new()?;
		let config = DirectoryConfig {
			write_key: "test-key".to_string(),
			storage_location: temp_dir.path().to_owned(),
			base_filename: "events".to_string(),
			max_file_size: 1024 * 2, // Tiny 2KB files for maximum pain
		};

		let store = DirectoryStore::new(config)?;
		let db = Arc::new(TransientDB::new(store));
		let mut handles = vec![];

		assert_eq!(db.transaction_type(), DataTransactionType::File);

		// Evil JSON generator (same as before)
		let generate_evil_json = |i: u64| -> Value {
			match i % 5 {
				0 => json!({
					"normal": "boring",
					"timestamp": chrono::Utc::now().to_rfc3339(),
				}),
				1 => json!({
					"nested": {
						"deeply": {
							"nested": {
								"value": "here",
								"with": ["arrays", "of", "doom", "that", "go", "on", "forever"]
							}
						}
					}
				}),
				2 => json!({
					"unicode": "🦀💥👾👿",
					"weird_spaces": "    \n\t\r    ",
					"empty": "",
				}),
				3 => {
					let mut huge = json!({});
					for n in 0..100 {
						huge[format!("field_{}", n)] = json!("value");
					}
					huge
				}
				_ => {
					let mut tiny = json!({});
					tiny["x"] = json!("y");
					tiny
				}
			}
		};

		let random_delay = || {
			let mut rng = rand::thread_rng();
			if rng.gen_bool(0.2) {
				thread::sleep(std::time::Duration::from_millis(rng.gen_range(0..50)));
			}
		};

		// Spawn append threads
		for _t in 0..20 {
			let db = db.clone();
			handles.push(thread::spawn(move || {
				let mut rng = rand::thread_rng();
				for i in 0..500 {
					let evil_json = generate_evil_json(i as u64);
					random_delay();
					let _ = db.append(evil_json);

					if i % 50 == 0 {
						if let Ok(Some(result)) = db.fetch(Some(rng.gen_range(0..5)), None) {
							if let Some(removable) = result.removable {
								let _ = db.remove(&removable);
							}
						}
					}
				}
			}));
		}

		// Monitoring thread now checks has_data() instead of count
		let db = db.clone();
		let db_verification = db.clone();
		handles.push(thread::spawn(move || {
			for _ in 0..1000 {
				assert!(
					db.has_data() || !db.has_data(),
					"Store should be in valid state"
				);
				thread::sleep(std::time::Duration::from_millis(1));
			}
		}));

		for handle in handles {
			handle.join().unwrap();
		}

		// Final verification
		db_verification.append(json!({"final": "test"}))?;
		assert!(
			db_verification.has_data(),
			"Store should have data after final append"
		);

		Ok(())
	}

	#[test]
	fn test_directory_store_malformed_files() -> io::Result<()> {
		let temp_dir = TempDir::new()?;
		let config = DirectoryConfig {
			write_key: "test-key".to_string(),
			storage_location: temp_dir.path().to_owned(),
			base_filename: "events".to_string(),
			max_file_size: 1024,
		};

		let store = DirectoryStore::new(config)?;
		let db = Arc::new(TransientDB::new(store));

		// First add some valid data
		db.append(json!({"valid": "data"}))?;

		// Force a fetch to complete the current file
		let _ = db.fetch(None, None)?;

		// Create some malformed files in the directory with .temp extension
		let malformed_path = temp_dir.path().join("999-events.temp");
		std::fs::write(&malformed_path, "{ invalid json")?;

		let empty_path = temp_dir.path().join("998-events.temp");
		std::fs::write(&empty_path, "")?;

		// Add more valid data
		db.append(json!({"more": "valid data"}))?;

		// Verify the store is still functional
		assert!(db.has_data(), "Store should still have data");

		// Check if we can still fetch data
		if let Some(result) = db.fetch(None, None)? {
			if let Some(files) = result.data_files {
				assert!(!files.is_empty(), "Should have at least one file");

				// Count valid files
				let valid_files = files
					.iter()
					.filter(|f| {
						if let Ok(content) = std::fs::read_to_string(f) {
							content.starts_with("{ \"batch\": [")
						} else {
							false
						}
					})
					.count();

				assert!(valid_files > 0, "Should have at least one valid file");
			}
		}

		Ok(())
	}

	#[test]
	fn test_transient_db_panic_recovery() -> io::Result<()> {
		use std::panic::{catch_unwind, AssertUnwindSafe};

		let config = MemoryConfig {
			write_key: "test-key".to_string(),
			max_items: 100,
			max_fetch_size: 1024,
		};

		let store = MemoryStore::new(config);
		let db = Arc::new(TransientDB::new(store));

		// Simulate a panic in one thread
		let db_panic = db.clone();
		let panic_result = catch_unwind(AssertUnwindSafe(|| {
			let _ = db_panic.append(json!({"before": "panic"}));
			panic!("Simulated panic");
		}));
		assert!(panic_result.is_err());

		// DB should still be usable
		db.append(json!({"after": "panic"}))?;
		assert!(db.has_data());

		Ok(())
	}

	#[test]
	fn test_directory_store_concurrent_validation() -> io::Result<()> {
		let temp_dir = TempDir::new()?;
		let config = DirectoryConfig {
			write_key: "test-key".to_string(),
			storage_location: temp_dir.path().to_owned(),
			base_filename: "events".to_string(),
			max_file_size: 256, // Small size to force more file rotations
		};

		let mut store = DirectoryStore::new(config)?;

		// Track validation calls with atomic counter
		let validation_count = Arc::new(AtomicUsize::new(0));
		let validation_count_clone = validation_count.clone();

		store.set_file_validator(move |_| {
			validation_count_clone.fetch_add(1, Ordering::SeqCst);
			thread::sleep(Duration::from_millis(50));
			Ok(())
		});

		let db = Arc::new(TransientDB::new(store));
		let append_count = Arc::new(AtomicUsize::new(0));
		let mut handles = vec![];

		// Multiple append threads
		for thread_id in 0..3 {
			let db_append = db.clone();
			let append_counter = append_count.clone();
			handles.push(thread::spawn(move || {
				for i in 0..10 {
					let event = json!({
						"thread": thread_id,
						"index": i,
						"value": format!("data_{}_{}",thread_id, i)
					});
					if let Ok(()) = db_append.append(event) {
						append_counter.fetch_add(1, Ordering::SeqCst);
					}
					// Small sleep to reduce contention
					thread::sleep(Duration::from_millis(1));
				}
			}));
		}

		// Multiple fetch threads
		for _ in 0..2 {
			let db_fetch = db.clone();
			handles.push(thread::spawn(move || {
				for _ in 0..5 {
					if let Ok(Some(result)) = db_fetch.fetch(Some(2), None) {
						if let Some(removable) = result.removable {
							let _ = db_fetch.remove(&removable);
						}
					}
					thread::sleep(Duration::from_millis(10));
				}
			}));
		}

		// Monitor thread to check file uniqueness
		let _db_monitor = db.clone();
		let temp_dir_path = temp_dir.path().to_owned();
		handles.push(thread::spawn(move || {
			for _ in 0..10 {
				let files: Vec<_> = fs::read_dir(&temp_dir_path)
					.unwrap()
					.filter_map(Result::ok)
					.map(|e| e.path())
					.filter(|p| p.extension().and_then(|ext| ext.to_str()) == Some("temp"))
					.collect();

				// Check for unique indices
				let mut seen_indices = HashSet::new();
				for file in files {
					if let Some(file_name) = file.file_name().and_then(|n| n.to_str()) {
						if let Some(num_str) = file_name.split('-').next() {
							if let Ok(num) = num_str.parse::<u32>() {
								assert!(
									seen_indices.insert(num),
									"Duplicate file index found: {}",
									num
								);
							}
						}
					}
				}
				thread::sleep(Duration::from_millis(20));
			}
		}));

		// Wait for all operations to complete
		for handle in handles {
			handle.join().unwrap();
		}

		// Verify final state
		assert!(
			validation_count.load(Ordering::SeqCst) > 0,
			"No validations occurred"
		);
		assert!(
			append_count.load(Ordering::SeqCst) > 0,
			"No appends succeeded"
		);

		// Final fetch to verify data integrity
		if let Some(result) = db.fetch(None, None)? {
			if let Some(files) = result.data_files {
				// Verify file contents
				let mut seen_events = HashSet::new();
				for file in &files {
					let content = fs::read_to_string(file)?;
					let value: Value = serde_json::from_str(&content)?;
					if let Some(batch) = value["batch"].as_array() {
						for event in batch {
							if let (Some(thread_id), Some(index)) =
								(event["thread"].as_i64(), event["index"].as_i64())
							{
								seen_events.insert((thread_id, index));
							}
						}
					}
				}

				// Check we got events from at least one thread
				let thread_counts: HashSet<_> =
					seen_events.iter().map(|(thread_id, _)| thread_id).collect();
				assert!(!thread_counts.is_empty(), "No events were recorded");
			}
		}

		Ok(())
	}
}
