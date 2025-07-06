# cute-sqlite-kv

[![License: MPL 2.0](https://img.shields.io/badge/License-MPL_2.0-brightgreen.svg)](https://opensource.org/licenses/MPL-2.0)

`cute-sqlite-kv` is a simple, opinionated Rust wrapper for SQLite that provides persistent key-value storage. Designed for simplicity and reliability, it offers essential KV operations with minimal overhead.


## Installation

Add to your `Cargo.toml`:
```toml
[dependencies]
cute-sqlite-kv = "0.1"
```

## Usage

### Basic Operations
```rust
use cute_sqlite_kv::KVStore;
use std::path::Path;

// Create/load store from file
let filename = Path::new("mydata.db");
let kvstore = KVStore::new_from_file(filename).unwrap();

// Insert key-value pair
kvstore.insert("username", "alice").unwrap();

// Retrieve value
let result = kvstore.get("username").unwrap();
assert_eq!(result, Some("alice".to_string()));

// Delete key
kvstore.remove("username").unwrap();

// Check deletion
let result = kvstore.get("username").unwrap();
assert_eq!(result, None);
```

## API Reference

### Core Methods

| Method | Description | Returns |
|--------|-------------|---------|
| `new_from_file(path: &Path)` | Creates/opens store | `Result<KVStore>` |
| `insert(key: &str, value: &str)` | Stores key-value pair | `Result<()>` |
| `get(key: &str)` | Retrieves value | `Result<Option<String>>` |
| `remove(key: &str)` | Deletes key | `Result<()>` |


## Contributing

Contributions are welcome! Please open issues or PRs on our [GitHub repository](https://github.com/yourusername/cute-sqlite-kv).

## License

This project is licensed under the MPL 2.0 License - see the [LICENSE](LICENSE) file for details.