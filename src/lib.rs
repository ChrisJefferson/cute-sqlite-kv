//! This crate provides a very small and simple multi-process
//! persistent key-value store, using `SQLite` for storage.
//!
//! The code is intended to be as simple a wrapper around `SQLite`
//! (via rusqlite) as possible.
//!
//! Two stores are provided:
//!
//! - [`KVStore`] maps string keys to string values.
//! - [`BlobStore`] maps string keys to binary (`Vec<u8>`) values.
//!
//! Both are thin aliases for the generic [`Store`], which holds all the
//! logic; only the value type differs. Keys are always `&str`.
//!
//! The store can be used from multiple processes, and also opened
//! multiple times from the same process.
//!
//! While `SQLite` can be very quick, this key-value store is not
//! intended for high-performance situations, but when you need
//! something as simple as possible, but still correct. Please feel
//! free to take, extend, and modify this code for your own requirements!
//!
//! # One value type per database file
//!
//! A given database file should be used with a single store type. The
//! value column happily holds whatever you write (text or binary), but
//! reading a binary value back as a `String` (or a text value as bytes)
//! fails -- and, per the panic policy below, that failure is a panic.
//! So pick [`KVStore`] or [`BlobStore`] for a file and stick to it.
//!
//! # Errors and panics
//!
//! Opening a store (`new_from_file` / `new_in_memory`) returns a
//! `Result`, because a bad path or permissions is a normal thing a
//! caller might want to handle.
//!
//! Every other operation panics if the underlying `SQLite` call fails.
//! The reasoning: once the store is open, the only remaining failures
//! are catastrophic (disk full, corruption, the file vanished) and
//! there is no sensible recovery. A loud panic is better than a
//! silently dropped error. Lock contention between processes does
//! *not* cause a panic: a `busy_timeout` is set so writers wait for
//! the lock rather than failing.
//!
//! # Examples
//!
//! ```rust
//! use cute_sqlite_kv::KVStore;
//!
//! let kvstore = KVStore::new_in_memory().unwrap();
//!
//! kvstore.insert("key", "value");
//!
//! assert_eq!(kvstore.get("key"), Some("value".to_string()));
//!
//! kvstore.remove("key");
//!
//! assert_eq!(kvstore.get("key"), None);
//! ```
//!
//! Storing binary values with [`BlobStore`]:
//!
//! ```rust
//! use cute_sqlite_kv::BlobStore;
//!
//! let store = BlobStore::new_in_memory().unwrap();
//!
//! let bytes: &[u8] = &[0, 1, 2, 255];
//! store.insert("key", bytes);
//!
//! assert_eq!(store.get("key"), Some(bytes.to_vec()));
//! ```
//!
//! Creating a file-backed store:
//!
//! ```no_run
//! use cute_sqlite_kv::KVStore;
//!
//! let kvstore = KVStore::new_from_file("mydata.db").unwrap();
//! ```
use std::marker::PhantomData;
use std::path::Path;
use std::time::Duration;

use rusqlite::types::FromSql;
use rusqlite::{Connection, OptionalExtension, ToSql};

const KEY_COLUMN: &str = "KVStore_key";
const VAL_COLUMN: &str = "KVStore_val";
const TABLE: &str = "KVStore_table";

/// How long a connection waits for a database lock held by another
/// connection or process before giving up (and panicking).
///
/// Every operation here is a single autocommit statement, so two
/// writers simply serialise: the loser's busy-handler sleeps and retries
/// until the winner commits (typically microseconds). This timeout
/// therefore only matters when some *other* process holds the write lock
/// for a long time -- a long external transaction, or a hung/crashed
/// process leaving a stale lock. A genuine deadlock is not affected by
/// this value: `SQLite` returns `SQLITE_BUSY` immediately in that case
/// rather than waiting. We pick a generous timeout so a slow but live
/// lock-holder is tolerated, and only give up (panic) once a wait this
/// long suggests the holder is never going to release.
const BUSY_TIMEOUT: Duration = Duration::from_secs(30);

/// A type that can be used as the value of a [`Store`].
///
/// This ties the owned value type (what [`Store::get`] returns) to its
/// borrowed form (what [`Store::insert`] accepts). It is implemented for
/// `String` (borrowed as `str`) and `Vec<u8>` (borrowed as `[u8]`).
pub trait StoreValue: FromSql {
    /// The borrowed form accepted by [`Store::insert`]: `str` for
    /// `String`, `[u8]` for `Vec<u8>`.
    type Ref: ToSql + ?Sized;
}

impl StoreValue for String {
    type Ref = str;
}

impl StoreValue for Vec<u8> {
    type Ref = [u8];
}

/// A simple key-value store backed by `SQLite`, generic over its value
/// type.
///
/// You will normally use one of the aliases [`KVStore`] (string values)
/// or [`BlobStore`] (binary values) rather than naming `Store` directly.
pub struct Store<V> {
    connection: Connection,
    _marker: PhantomData<fn() -> V>,
}

/// A key-value store mapping string keys to string values.
pub type KVStore = Store<String>;

/// A key-value store mapping string keys to binary (`Vec<u8>`) values.
pub type BlobStore = Store<Vec<u8>>;

impl<V: StoreValue> Store<V> {
    /// Creates a new in-memory key-value store.
    ///
    /// An in-memory key-value store is in practice worse than
    /// a standard `HashMap` in every way, so the only use of this function
    /// is for creating a key value store for testing.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use cute_sqlite_kv::KVStore;
    ///
    /// let kvstore = KVStore::new_in_memory().unwrap();
    /// ```
    pub fn new_in_memory() -> rusqlite::Result<Store<V>> {
        let connection = Connection::open_in_memory()?;
        connection.busy_timeout(BUSY_TIMEOUT)?;
        let store = Store {
            connection,
            _marker: PhantomData,
        };
        store.create_table()?;
        Ok(store)
    }

    /// Creates a new store using a file as the storage.
    ///
    /// # Arguments
    ///
    /// * `filename` - The path to the file used as the storage for the store.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use cute_sqlite_kv::KVStore;
    ///
    /// let kvstore = KVStore::new_from_file("mydata.db").unwrap();
    /// ```
    pub fn new_from_file(filename: impl AsRef<Path>) -> rusqlite::Result<Store<V>> {
        let connection = Connection::open(filename)?;
        connection.busy_timeout(BUSY_TIMEOUT)?;
        let store = Store {
            connection,
            _marker: PhantomData,
        };
        store.create_table()?;
        Ok(store)
    }

    /// Internal function which ensures the store's table is created
    fn create_table(&self) -> rusqlite::Result<()> {
        self.connection.execute(
            &format!(
                "CREATE TABLE IF NOT EXISTS {TABLE} (
                {KEY_COLUMN} varchar PRIMARY KEY UNIQUE NOT NULL,
                {VAL_COLUMN}
            )"
            ),
            (),
        )?;
        Ok(())
    }

    /// Inserts a key-value pair in the store.
    /// Overwrites any existing value.
    ///
    /// # Arguments
    ///
    /// * `key` - The key for the value.
    /// * `value` - The value to be stored.
    ///
    /// # Panics
    ///
    /// Panics if the underlying `SQLite` write fails.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use cute_sqlite_kv::KVStore;
    ///
    /// let kvstore = KVStore::new_in_memory().unwrap();
    ///
    /// kvstore.insert("key", "value");
    /// ```
    pub fn insert(&self, key: &str, value: &V::Ref) {
        self.connection
            .execute(
                &format!("REPLACE INTO {TABLE} ({KEY_COLUMN}, {VAL_COLUMN}) VALUES (?, ?)"),
                rusqlite::params![key, value],
            )
            .expect("cute-sqlite-kv: insert failed");
    }

    /// Checks if a particular key is contained in the store.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to check for existence.
    ///
    /// # Panics
    ///
    /// Panics if the underlying `SQLite` query fails.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use cute_sqlite_kv::KVStore;
    ///
    /// let kvstore = KVStore::new_in_memory().unwrap();
    ///
    /// kvstore.insert("key", "value");
    ///
    /// assert!(kvstore.contains_key("key"));
    /// assert!(!kvstore.contains_key("nonexistent_key"));
    /// ```
    pub fn contains_key(&self, key: &str) -> bool {
        let exists: i64 = self
            .connection
            .query_row(
                &format!("SELECT EXISTS(SELECT 1 FROM {TABLE} WHERE {KEY_COLUMN} = ?)"),
                [key],
                |row| row.get(0),
            )
            .expect("cute-sqlite-kv: contains_key query failed");
        exists != 0
    }

    /// Retrieves the value for a given key from the store.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to retrieve the value for.
    ///
    /// # Panics
    ///
    /// Panics if the underlying `SQLite` query fails.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use cute_sqlite_kv::KVStore;
    ///
    /// let kvstore = KVStore::new_in_memory().unwrap();
    ///
    /// kvstore.insert("key", "value");
    ///
    /// assert_eq!(kvstore.get("key"), Some("value".to_string()));
    /// ```
    pub fn get(&self, key: &str) -> Option<V> {
        self.connection
            .query_row(
                &format!("SELECT {VAL_COLUMN} FROM {TABLE} WHERE {KEY_COLUMN} = ?"),
                [key],
                |row| row.get(0),
            )
            .optional()
            .expect("cute-sqlite-kv: get query failed")
    }

    /// Removes a key-value pair from the store,
    /// if present, and returns the old value if it existed.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to remove.
    ///
    /// # Panics
    ///
    /// Panics if the underlying `SQLite` write fails.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use cute_sqlite_kv::KVStore;
    ///
    /// let kvstore = KVStore::new_in_memory().unwrap();
    ///
    /// kvstore.insert("key", "value");
    ///
    /// assert_eq!(kvstore.remove("key"), Some("value".to_string()));
    ///
    /// assert_eq!(kvstore.get("key"), None);
    ///
    /// assert_eq!(kvstore.remove("key"), None);
    /// ```
    pub fn remove(&self, key: &str) -> Option<V> {
        self.connection
            .query_row(
                &format!("DELETE FROM {TABLE} WHERE {KEY_COLUMN} = ? RETURNING {VAL_COLUMN}"),
                [key],
                |row| row.get(0),
            )
            .optional()
            .expect("cute-sqlite-kv: remove failed")
    }

    /// Clears the entire table in the store.
    ///
    /// This method removes all key-value pairs from the table, effectively clearing the entire store.
    ///
    /// # Panics
    ///
    /// Panics if the underlying `SQLite` write fails.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use cute_sqlite_kv::KVStore;
    ///
    /// let kvstore = KVStore::new_in_memory().unwrap();
    ///
    /// kvstore.insert("key1", "value1");
    /// kvstore.insert("key2", "value2");
    ///
    /// kvstore.clear();
    ///
    /// assert_eq!(kvstore.get("key1"), None);
    /// assert_eq!(kvstore.get("key2"), None);
    /// ```
    pub fn clear(&self) {
        self.connection
            .execute(&format!("DELETE FROM {TABLE}"), ())
            .expect("cute-sqlite-kv: clear failed");
    }

    /// Checks if the store is empty.
    ///
    /// Note: Since the store can be used concurrently, the result of this method
    /// can be out of date almost immediately.
    ///
    /// # Panics
    ///
    /// Panics if the underlying `SQLite` query fails.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use cute_sqlite_kv::KVStore;
    ///
    /// let kvstore = KVStore::new_in_memory().unwrap();
    /// assert!(kvstore.is_empty());
    ///
    /// kvstore.insert("key", "value");
    /// assert!(!kvstore.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        let empty: i64 = self
            .connection
            .query_row(
                &format!("SELECT NOT EXISTS(SELECT 1 FROM {TABLE})"),
                [],
                |row| row.get(0),
            )
            .expect("cute-sqlite-kv: is_empty query failed");
        empty != 0
    }

    /// Returns the number of key-value pairs in the store.
    ///
    /// Note: Since the store can be used concurrently, the result of this method
    /// can be out of date almost immediately.
    ///
    /// # Panics
    ///
    /// Panics if the underlying `SQLite` query fails.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use cute_sqlite_kv::KVStore;
    ///
    /// let kvstore = KVStore::new_in_memory().unwrap();
    /// assert_eq!(kvstore.len(), 0);
    ///
    /// kvstore.insert("key1", "value1");
    /// kvstore.insert("key2", "value2");
    /// assert_eq!(kvstore.len(), 2);
    /// ```
    pub fn len(&self) -> usize {
        let count: i64 = self
            .connection
            .query_row(&format!("SELECT COUNT(*) FROM {TABLE}"), [], |row| {
                row.get(0)
            })
            .expect("cute-sqlite-kv: len query failed");
        count as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::tempdir;

    #[test]
    fn test_new_in_memory() {
        let _ = KVStore::new_in_memory().unwrap();
    }

    #[test]
    fn test_new_from_file() {
        let temp_dir = tempdir().expect("Failed to create temp directory");
        let filename = temp_dir.path().join("kvstore.db");
        let _ = KVStore::new_from_file(&filename).unwrap();
    }

    #[test]
    fn test_new_from_file_more() {
        let temp_dir = tempdir().expect("Failed to create temp directory");
        let filename = temp_dir.path().join("kvstore.db");
        let kvstore = KVStore::new_from_file(&filename).unwrap();
        let key = "test_key";
        let value = "test_value";
        kvstore.insert(key, value);
        let result = kvstore.get(key);
        assert_eq!(result, Some(value.to_string()));
    }

    #[test]
    fn test_reopen_database() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");
        let filename = temp_dir.path().join("kvstore.db");
        {
            let kvstore = KVStore::new_from_file(&filename).unwrap();
            let key = "test_key";
            let value = "test_value";
            kvstore.insert(key, value);
        }
        {
            let kvstore = KVStore::new_from_file(&filename).unwrap();
            let key = "test_key";
            let result = kvstore.get(key);
            assert_eq!(result, Some("test_value".to_string()));
        }
    }

    #[test]
    fn test_insert_and_get() {
        let kvstore = KVStore::new_in_memory().unwrap();
        let key = "test_key";
        let value = "test_value";
        kvstore.insert(key, value);
        let result = kvstore.get(key);
        assert_eq!(result, Some(value.to_string()));
    }

    #[test]
    fn test_get_nonexistent_key() {
        let kvstore = KVStore::new_in_memory().unwrap();
        let key = "nonexistent_key";
        let result = kvstore.get(key);
        assert_eq!(result, None);
    }

    #[test]
    fn test_remove() {
        let kvstore = KVStore::new_in_memory().unwrap();
        let key = "test_key";
        let value = "test_value";
        kvstore.insert(key, value);
        let old_value = kvstore.remove(key);
        assert_eq!(old_value, Some(value.to_string()));
        let result = kvstore.get(key);
        assert_eq!(result, None);
    }

    #[test]
    fn test_remove_nonexistent_key() {
        let kvstore = KVStore::new_in_memory().unwrap();
        let key = "nonexistent_key";
        let old_value = kvstore.remove(key);
        assert_eq!(old_value, None);
        let result = kvstore.get(key);
        assert_eq!(result, None);
    }

    #[test]
    fn test_clear() {
        let kvstore = KVStore::new_in_memory().unwrap();
        let key = "test_key";
        let value = "test_value";
        kvstore.insert(key, value);
        kvstore.clear();
        let result = kvstore.get(key);
        assert_eq!(result, None);
    }

    #[test]
    fn test_many_connections() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");
        let filename = temp_dir.path().join("kvstore.db");

        // Create the first connection and add a key
        {
            let kvstore = KVStore::new_from_file(&filename).unwrap();
            let key = "test_key";
            let value = "test_value";
            kvstore.insert(key, value);
        }

        // Check if the key is there
        {
            let kvstore = KVStore::new_from_file(&filename).unwrap();
            let key = "test_key";
            let result = kvstore.get(key);
            assert_eq!(result, Some("test_value".to_string()));
        }

        // remove the key
        {
            let kvstore = KVStore::new_from_file(&filename).unwrap();
            let key = "test_key";
            kvstore.remove(key);
        }

        // Check if the key is gone
        {
            let kvstore = KVStore::new_from_file(&filename).unwrap();
            let key = "test_key";
            let result = kvstore.get(key);
            assert_eq!(result, None);
        }
    }

    #[test]
    fn test_overlapping_connections() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");
        let filename = temp_dir.path().join("kvstore.db");

        let kvstore = KVStore::new_from_file(&filename).unwrap();

        // Create the first connection and add a key
        {
            let key = "test_key";
            let value = "test_value";
            kvstore.insert(key, value);
        }

        let kvstore2 = KVStore::new_from_file(&filename).unwrap();

        // Check if the key is there
        {
            let key = "test_key";
            let result = kvstore2.get(key);
            assert_eq!(result, Some("test_value".to_string()));
        }

        // remove the key
        {
            let key = "test_key";
            kvstore2.remove(key);
        }

        // Check if the key is gone
        {
            let key = "test_key";
            let result = kvstore.get(key);
            assert_eq!(result, None);
        }
    }

    #[test]
    fn test_insert_multiple_times() {
        let kvstore = KVStore::new_in_memory().unwrap();
        let key = "test_key";
        let value1 = "test_value1";
        let value2 = "test_value2";
        let value3 = "test_value3";

        kvstore.insert(key, value1);
        let result1 = kvstore.get(key);
        assert_eq!(result1, Some(value1.to_string()));

        kvstore.insert(key, value2);
        let result2 = kvstore.get(key);
        assert_eq!(result2, Some(value2.to_string()));

        kvstore.insert(key, value3);
        let result3 = kvstore.get(key);
        assert_eq!(result3, Some(value3.to_string()));
    }

    #[test]
    fn test_blob_roundtrip() {
        let store = BlobStore::new_in_memory().unwrap();
        // Deliberately not valid UTF-8.
        let value: &[u8] = &[0u8, 159, 146, 150, 255];
        store.insert("key", value);
        assert_eq!(store.get("key"), Some(value.to_vec()));
        assert_eq!(store.remove("key"), Some(value.to_vec()));
        assert_eq!(store.get("key"), None);
    }

    #[test]
    fn test_blob_reopen() {
        let temp_dir = tempdir().expect("Failed to create temp directory");
        let filename = temp_dir.path().join("blobstore.db");
        let value: &[u8] = &[10, 20, 0, 255, 128];
        {
            let store = BlobStore::new_from_file(&filename).unwrap();
            store.insert("key", value);
        }
        {
            let store = BlobStore::new_from_file(&filename).unwrap();
            assert_eq!(store.get("key"), Some(value.to_vec()));
        }
    }
}
