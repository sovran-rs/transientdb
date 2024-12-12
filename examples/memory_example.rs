use serde_json::json;
use std::io::Result;
use std::thread;
use std::time::Duration;
use transientdb::{MemoryConfig, MemoryStore, TransientDB};

fn memory_store_example() -> Result<()> {
	// Create a memory store configuration
	let config = MemoryConfig {
		write_key: "analytics-key".into(),
		max_items: 1000,           // Store up to 1000 items
		max_fetch_size: 1024 * 64, // 64KB max fetch size
	};

	// Initialize the store and wrap it in TransientDB
	let store = MemoryStore::new(config);
	let db = TransientDB::new(store);

	// Simulate collecting user events
	println!("Adding user events...");
	for i in 0..5 {
		let event = json!({
			"event_type": "page_view",
			"user_id": format!("user_{}", i),
			"timestamp": chrono::Utc::now().to_rfc3339(),
			"page": format!("/product/{}", i),
			"metadata": {
				"browser": "Chrome",
				"platform": "macOS",
				"screen_size": "1920x1080"
			}
		});

		db.append(event)?;
		thread::sleep(Duration::from_millis(100)); // Simulate time between events
	}

	// Fetch events in batches
	println!("Fetching and processing events...");
	while let Some(result) = db.fetch(Some(2), None)? {
		if let Some(data) = result.data {
			println!("Processing batch: {}", serde_json::to_string_pretty(&data)?);

			// After processing, remove the fetched items
			if let Some(removable) = result.removable {
				db.remove(&removable)?;
				println!("Removed processed events from store");
			}
		}
	}

	Ok(())
}

fn main() -> Result<()> {
	println!("=== Memory Store Example ===");
	memory_store_example()?;

	Ok(())
}
