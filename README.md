# TransientDB

TransientDB is a lightweight, thread-safe temporary data storage system designed for efficient handling of transient data in Rust applications. It provides flexible storage options with both in-memory and file-based implementations, making it ideal for scenarios requiring temporary data buffering, event batching, or intermediate data storage.

## Features

- **Multiple Storage Backends**
    - In-memory storage with configurable size limits
    - File-based storage with automatic file rotation
    - ... or make your own!

- **Thread-Safe Operations**
    - Concurrent append and fetch operations
    - Atomic file operations for the directory-based store
    - Robust error handling and recovery

- **Configurable Behavior**
    - Customizable maximum file/batch sizes
    - File validation hooks
    - Flexible fetch limits (count and size-based)

- **FIFO (First-In-First-Out) Data Management**
    - Automatic cleanup of old data
    - Efficient batch processing
    - Controlled memory usage

## Installation

Add TransientDB to your `Cargo.toml`:

```toml
[dependencies]
transientdb = "0.1.0"  # Replace with actual version
```

## Usage

### Memory Store Example

```rust
use transientdb::{MemoryConfig, MemoryStore, TransientDB};

// Configure an in-memory store
let config = MemoryConfig {
    write_key: "my-app".into(),
    max_items: 1000,
    max_fetch_size: 1024 * 1024, // 1MB
};

// Create the store
let store = MemoryStore::new(config);
let db = TransientDB::new(store);

// Append data
db.append(serde_json::json!({
    "event": "user_login",
    "timestamp": "2024-01-01T00:00:00Z"
}))?;

// Fetch data (up to 100 items)
if let Some(result) = db.fetch(Some(100), None)? {
    if let Some(data) = result.data {
        // Process the batch of data
        let batch: serde_json::Value = serde_json::from_slice(&data)?;
        // ... handle the batch ...
    }
}
```

### Directory Store Example

```rust
use std::path::PathBuf;
use transientdb::{DirectoryConfig, DirectoryStore, TransientDB};

// Configure a file-based store
let config = DirectoryConfig {
    write_key: "my-app".into(),
    storage_location: PathBuf::from("/tmp/events"),
    base_filename: "batch".into(),
    max_file_size: 1024 * 1024, // 1MB
};

// Create the store with custom validation
let mut store = DirectoryStore::new(config)?;
store.set_file_validator(|path| {
    // Custom validation logic
    let metadata = std::fs::metadata(path)?;
    if metadata.len() < 10 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "File too small"
        ));
    }
    Ok(())
});

let db = TransientDB::new(store);

// Append events
db.append(serde_json::json!({
    "event": "purchase",
    "amount": 99.99
}))?;

// Fetch completed files
if let Some(result) = db.fetch(None, None)? {
    if let Some(files) = result.data_files {
        // Process the files
        for file in files {
            // ... handle each file ...
        }
    }
    // Clean up processed files
    if let Some(removable) = result.removable {
        db.remove(&removable)?;
    }
}
```

## Storage Implementations

### MemoryStore
- Stores data in memory using a FIFO queue
- Automatically removes old items when max_items is reached
- Ideal for high-throughput, temporary data storage

### DirectoryStore
- Stores data in rotating files in a specified directory
- Automatic file management and rotation
- Supports custom file validation
- Ideal for larger datasets and persistent storage needs

## Configuration Options

### MemoryConfig
- `write_key`: Identifier for the data source
- `max_items`: Maximum number of items to store
- `max_fetch_size`: Maximum size in bytes for a single fetch operation

### DirectoryConfig
- `write_key`: Identifier for the data source
- `storage_location`: Directory path for storing files
- `base_filename`: Base name for generated files
- `max_file_size`: Maximum size in bytes for individual files

## Thread Safety

TransientDB is designed to be thread-safe and can handle concurrent operations from multiple threads. The `TransientDB` struct wraps the storage implementation in a `Mutex` to ensure thread-safe access to the underlying store.

## Error Handling

All operations that could fail return `Result<T, std::io::Error>`. The library includes comprehensive error handling and recovery mechanisms, especially for file-based operations.

## Testing

The library includes an extensive test suite covering:
- Basic operations
- Concurrent access
- Edge cases
- Error conditions
- Recovery scenarios
- Performance under load

Run the tests using:

```bash
cargo test
```

## License

Copyright 2024 Sovran.la, Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

