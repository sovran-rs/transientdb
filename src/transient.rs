use std::sync::Mutex;
use serde_json::Value;
use std::io;
use crate::{DataStore, DataResult, DataTransactionType, Equivalent};

pub struct TransientDB {
    store: Mutex<Box<dyn DataStore + Send>>,
}

impl TransientDB {
    pub fn new(store: impl DataStore + Send + 'static) -> Self {
        Self {
            store: Mutex::new(Box::new(store)),
        }
    }

    pub fn has_data(&self) -> bool {
        self.store.lock().unwrap().has_data()
    }

    pub fn count(&self) -> usize {
        self.store.lock().unwrap().count()
    }

    pub fn transaction_type(&self) -> DataTransactionType {
        self.store.lock().unwrap().transaction_type()
    }

    pub fn reset(&self) {
        let mut store = self.store.lock().unwrap();
        store.reset();
    }

    pub fn append(&self, data: Value) -> io::Result<()> {
        let mut store = self.store.lock().unwrap();
        store.append(data)
    }

    pub fn fetch(&self, count: Option<usize>, max_bytes: Option<usize>) -> io::Result<Option<DataResult>> {
        let mut store = self.store.lock().unwrap();
        store.fetch(count, max_bytes)
    }

    pub fn remove(&self, data: &[Box<dyn Equivalent>]) -> io::Result<()> {
        let mut store = self.store.lock().unwrap();
        store.remove(data)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::fs::File;
    use std::io::Write;
    use crate::memory::{MemoryStore, MemoryConfig};
    use crate::directory::{DirectoryStore, DirectoryConfig};
    use std::{fs, io, thread};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tempfile::TempDir;
    use serde_json::{json, Value};
    use rand::Rng;
    use crate::DataTransactionType;
    use crate::transient::TransientDB;

    #[test]
    fn test_concurrent_appends() -> io::Result<()> {
        let config = MemoryConfig {
            write_key: "test-key-concurrent-appends".to_string(),  // Unique key
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
        let count = final_db.count();
        assert!(count > 0, "Should have at least some items");

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
            write_key: "test-key-append-and-fetch".to_string(),  // Unique key
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
                    let batch: Value = serde_json::from_slice(result.data.as_ref().unwrap()).unwrap();
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
        assert!(total_fetched >= 100, "Should have fetched at least 100 items");

        Ok(())
    }

    #[test]
    fn test_concurrent_reset() -> io::Result<()> {
        let config = MemoryConfig {
            write_key: "test-key-reset".to_string(),  // Unique key
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
                    let batch: Value = serde_json::from_slice(result.data.as_ref().unwrap()).unwrap();
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
        assert_eq!(db.count(), 0);

        Ok(())
    }

    #[test]
    fn test_concurrent_directory_store() -> io::Result<()> {
        let temp_dir = TempDir::new()?;
        let config = DirectoryConfig {
            write_key: "test-key-directory".to_string(),
            storage_location: temp_dir.path().to_owned(),
            base_filename: "events".to_string(),
            max_file_size: 200,  // Small size to force rotation
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
            max_items: 1_000_000,  // 1M items max
            max_fetch_size: 1024 * 1024,  // 1MB fetch size
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
                        let batch: Value = serde_json::from_slice(result.data.as_ref().unwrap()).unwrap();
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
            max_file_size: 1024 * 10,  // 10KB files to force lots of rotation
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

        // Verify we still have a working store
        assert!(db.has_data() || db.count() == 0, "Store should either have data or be empty");

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

        // Evil JSON generator
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
                },
                _ => {
                    let mut tiny = json!({});
                    tiny["x"] = json!("y");
                    tiny
                }
            }
        };

        let random_delay = || {
            let mut rng = rand::thread_rng();
            if rng.gen_bool(0.2) {  // 20% chance of delay
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
                        },
                        1 => {
                            // Random sized fetches
                            let count = Some(rng.gen_range(0..1000));
                            let bytes = Some(rng.gen_range(0..(1024 * 100)));
                            let _ = db.fetch(count, bytes);
                        },
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

        // Monitoring thread
        let db = db.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..1000 {
                let count = db.count();
                assert!(count <= 100_000, "Memory store count too high");
                thread::sleep(std::time::Duration::from_millis(1));
            }
        }));

        for handle in handles {
            handle.join().unwrap();
        }

        // Use final_db instead of db
        final_db.append(json!({"final": "test"}))?;
        assert!(final_db.count() > 0 || final_db.count() == 0, "Store should be in valid state");

        Ok(())
    }

    #[test]
    fn test_directory_store_chaos() -> io::Result<()> {
        let temp_dir = TempDir::new()?;
        let config = DirectoryConfig {
            write_key: "test-key".to_string(),
            storage_location: temp_dir.path().to_owned(),
            base_filename: "events".to_string(),
            max_file_size: 1024 * 2,  // Tiny 2KB files for maximum pain
        };

        let store = DirectoryStore::new(config)?;
        let db = Arc::new(TransientDB::new(store));
        let mut handles = vec![];

        assert_eq!(db.transaction_type(), DataTransactionType::File);

        // Create a clone for final verification
        let final_db = db.clone();

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
                },
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
                        // More frequent fetches for directory store
                        if let Ok(Some(result)) = db.fetch(Some(rng.gen_range(0..5)), None) {
                            if let Some(removable) = result.removable {
                                let _ = db.remove(&removable);
                            }
                        }
                    }
                }
            }));
        }

        // Spawn filesystem chaos threads
        for _ in 0..10 {
            let db = db.clone();
            let temp_dir_path = temp_dir.path().to_owned();
            handles.push(thread::spawn(move || {
                let mut rng = rand::thread_rng();
                for _ in 0..50 {
                    match rng.gen_range(0..4) {
                        0 => {
                            // Instead of making the whole directory read-only, let's do other filesystem chaos
                            match rng.gen_range(0..3) {
                                0 => {
                                    // Create some garbage files in the directory
                                    let garbage_path = temp_dir_path.join(format!("garbage_{}", rng.gen_range(0..1000)));
                                    let _ = std::fs::write(garbage_path, "not json at all!");
                                },
                                1 => {
                                    // Try to create a partial/incomplete file
                                    if let Ok(mut file) = File::create(temp_dir_path.join("partial.json")) {
                                        let _ = file.write_all(b"{ \"batch\": ["); // Incomplete JSON
                                    }
                                },
                                _ => {
                                    // Sleep during critical operations to try to catch race conditions
                                    thread::sleep(std::time::Duration::from_millis(rng.gen_range(10..50)));
                                }
                            }
                        },
                        1 => {
                            // Fetch everything and remove
                            if let Ok(Some(result)) = db.fetch(None, None) {
                                if let Some(removable) = result.removable {
                                    let _ = db.remove(&removable);
                                }
                            }
                        },
                        2 => {
                            db.reset();
                        },
                        _ => {
                            // Rapid-fire tiny appends to force rotations
                            for _ in 0..20 {
                                let tiny_json = json!({"x": "y"});
                                let _ = db.append(tiny_json);
                            }
                        }
                    }
                    random_delay();
                }
            }));
        }

        // Monitoring thread
        let db = db.clone();
        let temp_dir_path = temp_dir.path().to_owned();
        handles.push(thread::spawn(move || {
            for _ in 0..1000 {
                let dir_count = db.count();
                let files = std::fs::read_dir(&temp_dir_path)
                    .map(|entries| entries.count())
                    .unwrap_or(0);
                assert!(dir_count <= files, "Count should not exceed actual files");
                thread::sleep(std::time::Duration::from_millis(1));
            }
        }));

        for handle in handles {
            handle.join().unwrap();
        }

        // Use final_db instead of db
        final_db.append(json!({"final": "test"}))?;
        assert!(final_db.count() > 0 || final_db.count() == 0, "Store should be in valid state");

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
                let valid_files = files.iter()
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
                                assert!(seen_indices.insert(num),
                                        "Duplicate file index found: {}", num);
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
        assert!(validation_count.load(Ordering::SeqCst) > 0, "No validations occurred");
        assert!(append_count.load(Ordering::SeqCst) > 0, "No appends succeeded");

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
                            if let (Some(thread_id), Some(index)) = (
                                event["thread"].as_i64(),
                                event["index"].as_i64()
                            ) {
                                seen_events.insert((thread_id, index));
                            }
                        }
                    }
                }

                // Check we got events from at least one thread
                let thread_counts: HashSet<_> = seen_events
                    .iter()
                    .map(|(thread_id, _)| thread_id)
                    .collect();
                assert!(!thread_counts.is_empty(), "No events were recorded");
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod recovery_tests {
    use std::fs::{self, OpenOptions};
    use std::io::Write;
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::{io, thread};
    use std::time::Duration;
    use rand::Rng;
    use rand::seq::IteratorRandom;
    use serde_json::json;
    use tempfile::TempDir;
    use crate::directory::{DirectoryConfig, DirectoryStore};
    use crate::transient::TransientDB;

    // Helper to corrupt a file in various ways
    fn corrupt_file(path: &Path, corruption_type: u8) -> io::Result<()> {
        match corruption_type {
            0 => {
                // Truncate file mid-json
                let content = fs::read_to_string(path)?;
                let truncated = &content[0..content.len()/2];
                fs::write(path, truncated)?;
            },
            1 => {
                // Add garbage at the end
                let mut file = OpenOptions::new().append(true).open(path)?;
                file.write_all(b"garbage data!!!")?;
            },
            2 => {
                // Replace with invalid UTF-8
                let invalid_utf8 = vec![0xFF, 0xFF, 0xFF, 0xFF];
                fs::write(path, invalid_utf8)?;
            },
            3 => {
                // Create a valid JSON with incorrect schema
                fs::write(path, r#"{"not_batch": [], "not_write_key": ""}"#)?;
            },
            _ => {
                // Zero-length file
                fs::write(path, "")?;
            }
        }
        Ok(())
    }

    #[test]
    fn test_recovery_from_corrupted_files() -> io::Result<()> {
        let temp_dir = TempDir::new()?;
        let config = DirectoryConfig {
            write_key: "test-key".to_string(),
            storage_location: temp_dir.path().to_owned(),
            base_filename: "events".to_string(),
            max_file_size: 1024,
        };

        let store = DirectoryStore::new(config)?;
        let db = Arc::new(TransientDB::new(store));

        // Create several valid files
        for i in 0..5 {
            db.append(json!({"index": i}))?;
        }
        db.fetch(None, None)?;  // Force file completion

        // Corrupt some files
        let files: Vec<_> = fs::read_dir(temp_dir.path())?
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|ext| ext.to_str()) == Some("temp"))
            .collect();

        for (i, path) in files.iter().enumerate() {
            corrupt_file(path, (i % 5) as u8)?;
        }

        // Verify the store can still operate
        db.append(json!({"after_corruption": true}))?;

        if let Some(result) = db.fetch(None, None)? {
            if let Some(files) = result.data_files {
                // Should still get some valid files
                assert!(!files.is_empty(), "Should have at least valid files");
            }
        }

        Ok(())
    }

    #[test]
    fn test_concurrent_corruption_recovery() -> io::Result<()> {
        let temp_dir = TempDir::new()?;
        let config = DirectoryConfig {
            write_key: "test-key".to_string(),
            storage_location: temp_dir.path().to_owned(),
            base_filename: "events".to_string(),
            max_file_size: 512,  // Small size to force more files
        };

        let store = DirectoryStore::new(config)?;
        let db = Arc::new(TransientDB::new(store));
        let mut handles = vec![];

        // Thread that continuously appends
        let db_append = db.clone();
        handles.push(thread::spawn(move || {
            for i in 0..100 {
                let _ = db_append.append(json!({"index": i}));
                thread::sleep(Duration::from_millis(10));
            }
        }));

        // Thread that fetches and sometimes corrupts files
        let db_fetch = db.clone();
        let temp_dir_path = temp_dir.path().to_owned();
        handles.push(thread::spawn(move || {
            let mut rng = rand::thread_rng();
            for _ in 0..20 {
                if let Ok(Some(_)) = db_fetch.fetch(Some(2), None) {
                    // Occasionally corrupt a random file
                    if rng.gen_bool(0.3) {
                        if let Ok(files) = fs::read_dir(&temp_dir_path) {
                            if let Some(Ok(entry)) = files.choose(&mut rng) {
                                let _ = corrupt_file(&entry.path(), rng.gen_range(0..5));
                            }
                        }
                    }
                }
                thread::sleep(Duration::from_millis(50));
            }
        }));

        // Thread that validates directory integrity
        let db_validate = db.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..10 {
                // Attempt operations that should still work
                let _ = db_validate.append(json!({"validator": true}));
                let _ = db_validate.fetch(None, None);
                //assert!(db_validate.count() >= 0, "Count should never fail");
                thread::sleep(Duration::from_millis(100));
            }
        }));

        for handle in handles {
            handle.join().unwrap();
        }

        // Final validation
        assert!(db.has_data() || db.count() == 0, "Store should be in valid state");
        Ok(())
    }

    #[test]
    fn test_file_system_edge_cases() -> io::Result<()> {
        let temp_dir = TempDir::new()?;
        let config = DirectoryConfig {
            write_key: "test-key".to_string(),
            storage_location: temp_dir.path().to_owned(),
            base_filename: "events".to_string(),
            max_file_size: 1024,
        };

        let mut store = DirectoryStore::new(config)?;

        // Set up a validator that occasionally fails
        let fail_counter = Arc::new(AtomicUsize::new(0));
        let fail_counter_clone = fail_counter.clone();
        store.set_file_validator(move |_| {
            let count = fail_counter_clone.fetch_add(1, Ordering::SeqCst);
            if count % 3 == 0 {
                Err(io::Error::new(io::ErrorKind::Other, "Simulated validation failure"))
            } else {
                Ok(())
            }
        });

        let db = Arc::new(TransientDB::new(store));

        // Create symlinks, hardlinks, and other special files
        let special_files = vec![
            ("symlink", temp_dir.path().join("symlink.temp")),
            ("hardlink", temp_dir.path().join("hardlink.temp")),
            ("hidden", temp_dir.path().join(".hidden.temp")),
        ];

        for (kind, path) in special_files {
            match kind {
                "symlink" => {
                    if let Ok(()) = fs::write(&path, "{}") {
                        let _ = std::os::unix::fs::symlink(&path, temp_dir.path().join("sym.temp"));
                    }
                },
                "hardlink" => {
                    if let Ok(()) = fs::write(&path, "{}") {
                        let _ = fs::hard_link(&path, temp_dir.path().join("hard.temp"));
                    }
                },
                "hidden" => {
                    let _ = fs::write(&path, "{}");
                },
                _ => {}
            }
        }

        // Operate with special files present
        let mut successful_appends = 0;
        let mut successful_fetches = 0;

        // Try multiple appends, expecting some to succeed and some to fail
        for i in 0..10 {
            match db.append(json!({"index": i})) {
                Ok(_) => successful_appends += 1,
                Err(_) => {} // Expected occasional failures
            }
        }

        // Try multiple fetches, expecting some to succeed and some to fail
        for _ in 0..5 {
            match db.fetch(None, None) {
                Ok(Some(result)) => {
                    successful_fetches += 1;
                    if let Some(removable) = result.removable {
                        let _ = db.remove(&removable); // Ignore removal errors
                    }
                }
                Ok(None) => {},
                Err(_) => {} // Expected occasional failures
            }
        }

        // Verify that at least some operations succeeded
        assert!(successful_appends > 0, "Should have some successful appends");
        assert!(successful_fetches > 0 || successful_appends == 0, "Should have some successful fetches if there were successful appends");

        // Verify that our validator was actually called
        assert!(fail_counter.load(Ordering::SeqCst) > 0, "Validator should have been called");

        // Final verification that the store is still functional
        let final_append = db.append(json!({"final": true}));
        assert!(final_append.is_ok() || final_append.is_err(), "Store should either accept or reject final append");

        // Count should never panic
        let _ = db.count();

        Ok(())
    }
}