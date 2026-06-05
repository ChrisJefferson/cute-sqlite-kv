# cute-sqlite-kv

[![License: MPL 2.0](https://img.shields.io/badge/License-MPL_2.0-brightgreen.svg)](https://opensource.org/licenses/MPL-2.0)

`cute-sqlite-kv` is a simple, opinionated Rust wrapper for SQLite that provides a persistent, multi-process key-value store. It is designed to be as small and simple as possible while still being correct, rather than for high performance.

Two stores are provided: `KVStore` for string values and `BlobStore` for binary (`Vec<u8>`) values. Both are aliases for a generic `Store<V>` that holds all the logic — only the value type differs. Keys are always `&str`.


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

// Create/load a store from a file
let kvstore = KVStore::new_from_file("mydata.db").unwrap();

// Insert a key-value pair
kvstore.insert("username", "alice");

// Retrieve a value
assert_eq!(kvstore.get("username"), Some("alice".to_string()));

// Delete a key, getting back the old value (if any)
assert_eq!(kvstore.remove("username"), Some("alice".to_string()));

// It is now gone
assert_eq!(kvstore.get("username"), None);
```

### Binary values

Use `BlobStore` when values are arbitrary bytes rather than strings — for example a large string you have compressed with zstd before storing:

```rust
use cute_sqlite_kv::BlobStore;

let store = BlobStore::new_from_file("blobs.db").unwrap();

let bytes: &[u8] = &[0, 1, 2, 255];
store.insert("key", bytes);

assert_eq!(store.get("key"), Some(bytes.to_vec()));
```

A given database file should be used with a **single** store type. The value column holds whatever you write, but reading a binary value back as a `String` (or a text value as bytes) fails — and, per the panic policy below, that is a panic. So pick `KVStore` or `BlobStore` for a file and stick to it.

`cute-sqlite-kv` does not compress for you: that keeps the database inspectable in any SQLite tool and leaves the choice of codec to you. Compress before `insert` and decompress after `get`.

## Errors and panics

Opening a store (`new_from_file` / `new_in_memory`) returns a `Result`, because a bad path or permissions is something a caller may reasonably want to handle.

Every *other* operation panics if the underlying SQLite call fails. Once the store is open, the only remaining failures are catastrophic (disk full, corruption, the file vanished) with no sensible recovery, so a loud panic is preferred over a silently dropped error. Lock contention between processes does **not** panic: a busy-timeout is set, so a writer waits for the lock rather than failing.

The same file can be opened from multiple processes, and multiple times from the same process. File-backed stores use SQLite's WAL mode so reads can proceed during a write; this creates two sidecar files (`<file>-wal`, `<file>-shm`) and is not supported on network filesystems such as NFS. `KVStore` is `Send` but not `Sync` (SQLite connections are not `Sync`), so to use it from several threads, open one `KVStore` per thread rather than sharing one behind an `Arc`.

## API Reference

The table below shows the methods for `KVStore` (string values). For `BlobStore`, the value type changes: `insert` takes `&[u8]`, and `get`/`remove` return `Option<Vec<u8>>`.

| Method | Description | Returns |
|--------|-------------|---------|
| `new_from_file(path: impl AsRef<Path>)` | Creates/opens a file-backed store | `Result<KVStore>` |
| `new_in_memory()` | Creates an in-memory store (mainly for testing) | `Result<KVStore>` |
| `insert(key: &str, value: &str)` | Stores a key-value pair, overwriting any existing value | `()` |
| `get(key: &str)` | Retrieves a value | `Option<String>` |
| `contains_key(key: &str)` | Checks whether a key is present | `bool` |
| `remove(key: &str)` | Deletes a key, returning the old value | `Option<String>` |
| `clear()` | Removes all key-value pairs | `()` |
| `is_empty()` | Checks whether the store is empty | `bool` |
| `len()` | Number of key-value pairs | `usize` |


## Contributing

Contributions are welcome! Please open issues or PRs on the [GitHub repository](https://github.com/ChrisJefferson/cute-sqlite-kv).

## License

This project is licensed under the MPL 2.0 License - see the [LICENSE](LICENSE.txt) file for details.
