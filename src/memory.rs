use serde_json::Value;
use std::collections::VecDeque;
use std::io;
use serde_json::json;
use crate::{DataStore, DataResult, DataTransactionType, Equivalent};

pub struct MemoryConfig {
    pub write_key: String,
    pub max_items: usize,
    pub max_fetch_size: usize,
}

pub struct MemoryStore {
    config: MemoryConfig,
    items: VecDeque<Value>,
}

impl MemoryStore {
    pub fn new(config: MemoryConfig) -> Self {
        if config.max_fetch_size < 100 {
            panic!("max_fetch_size < 100 bytes? What are you even trying to fetch, empty arrays?");
        }
        if config.max_items == 0 {
            panic!("max_items = 0? So... you want a store that stores nothing? That's what /dev/null is for.");
        }

        Self {
            config,
            items: VecDeque::new(),
        }
    }

    fn create_batch(&self, items: &[Value]) -> Value {
        json!({
            "batch": items,
            "sentAt": chrono::Utc::now().to_rfc3339(),
            "writeKey": self.config.write_key
        })
    }

    fn get_item_size(item: &Value) -> usize {
        item.to_string().len()
    }
}

impl DataStore for MemoryStore {
    fn has_data(&self) -> bool {
        !self.items.is_empty()
    }

    fn count(&self) -> usize {
        self.items.len()
    }

    fn transaction_type(&self) -> DataTransactionType {
        DataTransactionType::Data
    }

    fn reset(&mut self) {
        self.items.clear();
    }

    fn append(&mut self, data: Value) -> io::Result<()> {
        self.items.push_back(data);

        while self.items.len() > self.config.max_items {
            self.items.pop_front();
        }

        Ok(())
    }

    fn fetch(&mut self, count: Option<usize>, max_bytes: Option<usize>) -> io::Result<Option<DataResult>> {
        if self.items.is_empty() {
            return Ok(None);
        }

        let max_bytes = max_bytes.unwrap_or(self.config.max_fetch_size);
        let mut accumulated_size = 0;
        let mut num_items = 0;

        // First, calculate sizes of raw items before batching
        for item in &self.items {
            let item_size = Self::get_item_size(item);
            if accumulated_size + item_size > max_bytes {
                break;
            }
            if let Some(count) = count {
                if num_items >= count {
                    break;
                }
            }
            accumulated_size += item_size;
            num_items += 1;
        }

        if num_items == 0 {
            return Ok(None);
        }

        // Collect items and create batch
        let items: Vec<Value> = self.items.drain(0..num_items).collect();
        let batch = self.create_batch(&items);
        let batch_data = serde_json::to_vec(&batch)?;

        Ok(Some(DataResult {
            data: Some(batch_data),
            data_files: None,
            removable: None,
        }))
    }

    fn remove(&mut self, _data: &[Box<dyn Equivalent>]) -> io::Result<()> {
        // No-op since fetch already removes the items
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use crate::DataStore;
    use serde_json::{json, Value};
    use crate::memory::{MemoryConfig, MemoryStore};

    #[test]
    fn test_basic_operations() -> io::Result<()> {
        let config = MemoryConfig {
            write_key: "test-key".to_string(),
            max_items: 1000,
            max_fetch_size: 1024,
        };

        let mut store = MemoryStore::new(config);

        // Test empty state
        assert_eq!(store.count(), 0);
        assert!(!store.has_data());

        // Test append
        let event = json!({"event": "test", "value": 123});
        store.append(event.clone())?;
        assert_eq!(store.count(), 1);
        assert!(store.has_data());

        // Test fetch
        if let Some(result) = store.fetch(None, None)? {
            let batch: Value = serde_json::from_slice(&result.data.unwrap())?;
            let items = batch["batch"].as_array().unwrap();
            assert_eq!(items.len(), 1);
            assert_eq!(items[0]["value"], 123);

            // Verify items were removed
            assert_eq!(store.count(), 0);
        } else {
            panic!("Expected data but got none");
        }

        Ok(())
    }

    #[test]
    fn test_fifo_behavior() -> io::Result<()> {
        let config = MemoryConfig {
            write_key: "test-key".to_string(),
            max_items: 3,  // Small limit to test FIFO
            max_fetch_size: 1024,
        };

        let mut store = MemoryStore::new(config);

        // Add more items than max_items
        for i in 0..5 {
            store.append(json!({"index": i}))?;
        }

        // Should only have last 3 items
        assert_eq!(store.count(), 3);

        // Verify they're the right items (2,3,4)
        if let Some(result) = store.fetch(None, None)? {
            let batch: Value = serde_json::from_slice(&result.data.unwrap())?;
            let items = batch["batch"].as_array().unwrap();
            assert_eq!(items.len(), 3);
            assert_eq!(items[0]["index"], 2);
            assert_eq!(items[1]["index"], 3);
            assert_eq!(items[2]["index"], 4);
        }

        Ok(())
    }

    #[test]
    fn test_fetch_limits() -> io::Result<()> {
        let config = MemoryConfig {
            write_key: "test-key".to_string(),
            max_items: 100,
            max_fetch_size: 1000,
        };

        let mut store = MemoryStore::new(config);

        // Add items with predictable sizes
        for i in 0..10 {
            let padding = "x".repeat(50);  // Each item will be roughly ~70 bytes
            store.append(json!({
            "index": i,
            "padding": padding
        }))?;
        }

        // Test count limit
        if let Some(result) = store.fetch(Some(3), None)? {
            let batch: Value = serde_json::from_slice(result.data.as_ref().unwrap())?;
            let items = batch["batch"].as_array().unwrap();
            assert_eq!(items.len(), 3, "Count limit not respected");
        }

        // Add more items for size limit test
        for i in 0..10 {
            let padding = "x".repeat(50);
            store.append(json!({
            "index": i,
            "padding": padding
        }))?;
        }

        // Test byte limit (200 bytes should get us about 2-3 items)
        if let Some(result) = store.fetch(None, Some(200))? {
            let items = serde_json::from_slice::<Value>(result.data.as_ref().unwrap())?;
            let items = items["batch"].as_array().unwrap();
            assert!(items.len() <= 3, "Too many items for byte limit");

            // Each raw item should be under the limit
            let total_raw_size: usize = items
                .iter()
                .map(|item| MemoryStore::get_item_size(item))
                .sum();
            assert!(total_raw_size <= 200, "Raw items exceed byte limit");
        }

        Ok(())
    }

    #[test]
    fn test_reset() -> io::Result<()> {
        let config = MemoryConfig {
            write_key: "test-key".to_string(),
            max_items: 100,
            max_fetch_size: 1000,
        };

        let mut store = MemoryStore::new(config);

        // Add some items
        for i in 0..5 {
            store.append(json!({"index": i}))?;
        }
        assert_eq!(store.count(), 5);

        // Reset and verify
        store.reset();
        assert_eq!(store.count(), 0);
        assert!(!store.has_data());

        Ok(())
    }

    #[test]
    fn test_memory_store_max_fetch_size_edge_cases() -> io::Result<()> {
        let config = MemoryConfig {
            write_key: "test-key".to_string(),
            max_items: 100,
            max_fetch_size: 300,  // Reasonable size that's easy to stay under/go over
        };

        let mut store = MemoryStore::new(config);

        // Small item ~30-40 bytes
        store.append(json!({"type": "small", "value": "tiny"}))?;

        // Large item ~500 bytes
        store.append(json!({
            "type": "large",
            "value": "x".repeat(400),  // Make it obviously too big
        }))?;

        // First fetch should only get the small item
        if let Some(result) = store.fetch(None, None)? {
            let batch: Value = serde_json::from_slice(&result.data.unwrap())?;
            let items = batch["batch"].as_array().unwrap();
            assert_eq!(items.len(), 1, "Should only fetch the small item");
            assert_eq!(items[0]["type"], "small");
        }

        // Second fetch should get the large item
        if let Some(result) = store.fetch(None, None)? {
            let batch: Value = serde_json::from_slice(&result.data.unwrap())?;
            let items = batch["batch"].as_array().unwrap();
            assert_eq!(items.len(), 1, "Should fetch the large item");
            assert_eq!(items[0]["type"], "large");
        }

        Ok(())
    }

    #[test]
    fn test_memory_store_json_types() -> io::Result<()> {
        let config = MemoryConfig {
            write_key: "test-key".to_string(),
            max_items: 100,
            max_fetch_size: 1024,
        };

        let mut store = MemoryStore::new(config);

        // Test all JSON types
        store.append(json!(null))?;
        store.append(json!(true))?;
        store.append(json!(42))?;
        store.append(json!(42.5))?;
        store.append(json!("string"))?;
        store.append(json!([1, 2, 3]))?;
        store.append(json!({"key": "value"}))?;

        if let Some(result) = store.fetch(None, None)? {
            let batch: Value = serde_json::from_slice(&result.data.unwrap())?;
            let items = batch["batch"].as_array().unwrap();
            assert_eq!(items.len(), 7, "All JSON types should be stored and retrieved");
        }

        Ok(())
    }

    #[test]
    #[should_panic(expected = "max_fetch_size < 100 bytes? What are you even trying to fetch, empty arrays?")]
    fn test_rejects_tiny_max_fetch_size() {
        let config = MemoryConfig {
            write_key: "test-key".to_string(),
            max_items: 1000,
            max_fetch_size: 50,  // Ridiculously small
        };

        let _store = MemoryStore::new(config);
    }

    #[test]
    #[should_panic(expected = "max_items = 0? So... you want a store that stores nothing? That's what /dev/null is for.")]
    fn test_rejects_zero_max_items() {
        let config = MemoryConfig {
            write_key: "test-key".to_string(),
            max_items: 0,  // Why even bother?
            max_fetch_size: 1024,
        };

        let _store = MemoryStore::new(config);
    }
}