use std::path::{Path, PathBuf};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use chrono::Utc;
use serde_json::Value;
use std::sync::atomic::{AtomicU32, Ordering};
use crate::{DataStore, DataResult, DataTransactionType, Equivalent};

pub struct DirectoryConfig {
    pub write_key: String,
    pub storage_location: PathBuf,
    pub base_filename: String,
    pub max_file_size: usize,
}

pub struct DirectoryStore {
    config: DirectoryConfig,
    writer: Option<BufWriter<File>>,
    current_size: usize,
    current_path: Option<PathBuf>,
    file_validator: Option<Box<dyn Fn(&Path) -> io::Result<()> + Send + Sync>>,
    next_index: AtomicU32,
}

impl DirectoryStore {
    const TEMP_EXTENSION: &'static str = "temp";

    pub fn new(config: DirectoryConfig) -> io::Result<Self> {
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

    pub fn set_file_validator<F>(&mut self, validator: F)
    where
        F: Fn(&Path) -> io::Result<()> + 'static + Send + Sync,
    {
        self.file_validator = Some(Box::new(validator));
    }

    fn next_index(&self) -> u32 {
        self.next_index.fetch_add(1, Ordering::SeqCst)
    }

    fn start_file_if_needed(&mut self) -> io::Result<bool> {
        if self.writer.is_some() {
            return Ok(false);
        }

        let mut index = self.next_index();
        let mut attempts = 0;
        const MAX_ATTEMPTS: u32 = 1000;  // Safeguard against infinite loops

        loop {
            let file_path = self.config.storage_location.join(
                format!("{}-{}", index, self.config.base_filename)
            );

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
                },
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                    // File exists, try next index
                    index = self.next_index();
                    attempts += 1;

                    if attempts >= MAX_ATTEMPTS {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            "Failed to find available index after maximum attempts"
                        ));
                    }
                },
                Err(e) => return Err(e),  // Other errors are propagated
            }
        }
    }

    fn finish_file(&mut self) -> io::Result<()> {
        let _ = match self.writer.take() {
            Some(mut writer) => {
                // Changed writeln! to write! to avoid newline
                write!(writer,
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

    fn sorted_files(&self, include_unfinished: bool) -> io::Result<Vec<PathBuf>> {
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

        files.sort_by(|a, b| a.file_name().unwrap_or_default().cmp(b.file_name().unwrap_or_default()));
        Ok(files)
    }

    fn up_to_size(&self, max_bytes: usize, files: &[PathBuf]) -> io::Result<Vec<PathBuf>> {
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
    fn has_data(&self) -> bool {
        self.count() > 0
    }

    fn count(&self) -> usize {
        fs::read_dir(&self.config.storage_location)
            .map(|entries| {
                entries
                    .filter_map(Result::ok)
                    .filter(|e| e.path().extension().and_then(|ext| ext.to_str()) == Some(Self::TEMP_EXTENSION))
                    .count()
            })
            .unwrap_or(0)
    }

    fn transaction_type(&self) -> DataTransactionType {
        DataTransactionType::File
    }

    fn reset(&mut self) {
        if let Ok(files) = self.sorted_files(true) {
            let _ = self.remove(&files.iter()
                .map(|p| Box::new(p.clone()) as Box<dyn Equivalent>)
                .collect::<Vec<_>>());
        }
    }

    fn append(&mut self, data: Value) -> io::Result<()> {
        let started = self.start_file_if_needed()?;
        let writer = self.writer.as_mut().ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, "No active writer")
        })?;

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

    fn fetch(&mut self, count: Option<usize>, max_bytes: Option<usize>) -> io::Result<Option<DataResult>> {
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

        let removable = files.iter()
            .map(|p| Box::new(p.clone()) as Box<dyn Equivalent>)
            .collect::<Vec<_>>();

        Ok(Some(DataResult {
            data: None,
            data_files: Some(files),
            removable: Some(removable),
        }))
    }

    fn remove(&mut self, data: &[Box<dyn Equivalent>]) -> io::Result<()> {
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
    use std::fs;
    use std::io;
    use super::{DirectoryConfig, DirectoryStore};
    use tempfile::TempDir;
    use crate::DataStore;

    #[test]
    fn test_directory_store_basic_operations() -> io::Result<()> {
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
            assert!(result.data_files.is_some());
            let files = result.data_files.unwrap();
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
    fn test_file_rotation() -> io::Result<()> {
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
            let files = result.data_files.unwrap();
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
    fn test_fetch_with_size_limit() -> io::Result<()> {
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
            let files = result.data_files.unwrap();
            let total_size: u64 = files.iter()
                .map(|f| fs::metadata(f).unwrap().len())
                .sum();
            assert!(total_size <= 250, "Fetch exceeded size limit");
        }

        Ok(())
    }

    #[test]
    fn test_file_cleanup() -> io::Result<()> {
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
            let files = result.data_files.unwrap();
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
    fn test_file_validator() -> io::Result<()> {
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
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "File too small"
                ));
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
    fn test_fetch_limits() -> io::Result<()> {
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
            let files = result.data_files.unwrap();
            assert_eq!(files.len(), 3, "Count limit not respected");
        }

        // Test byte limit only
        if let Some(result) = store.fetch(None, Some(250))? {
            let files = result.data_files.unwrap();
            let total_size: u64 = files.iter()
                .map(|f| fs::metadata(f).unwrap().len())
                .sum();
            assert!(total_size <= 250, "Byte limit not respected");
        }

        // Test both count and byte limits
        if let Some(result) = store.fetch(Some(5), Some(200))? {
            let files = result.data_files.unwrap();
            assert!(files.len() <= 5, "Count limit not respected in combined test");

            let total_size: u64 = files.iter()
                .map(|f| fs::metadata(f).unwrap().len())
                .sum();
            assert!(total_size <= 200, "Byte limit not respected in combined test");
        }

        Ok(())
    }

    #[test]
    #[should_panic(expected = "Seriously? max_file_size < 100 bytes? What exactly do you expect to store in there?")]
    fn test_rejects_tiny_max_file_size() {
        let temp_dir = TempDir::new().unwrap();
        let config = DirectoryConfig {
            write_key: "test-key".to_string(),
            storage_location: temp_dir.path().to_owned(),
            base_filename: "events".to_string(),
            max_file_size: 50,  // Try to use a ridiculously small size
        };

        // This should panic
        let _store = DirectoryStore::new(config).unwrap();
    }

    #[test]
    #[should_panic(expected = "Seriously? max_file_size < 100 bytes? What exactly do you expect to store in there?")]
    fn test_rejects_zero_max_file_size() {
        let temp_dir = TempDir::new().unwrap();
        let config = DirectoryConfig {
            write_key: "test-key".to_string(),
            storage_location: temp_dir.path().to_owned(),
            base_filename: "events".to_string(),
            max_file_size: 0,  // Try to use zero
        };

        // This should panic
        let _store = DirectoryStore::new(config).unwrap();
    }
}