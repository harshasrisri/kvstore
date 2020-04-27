pub use crate::kvls::KvLogStore;
use crate::Result;
use failure::err_msg;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::Path;

/// A KvStore is a type which holds a map of keys to values. Keys are unique
/// and map to values. A key-value pair can be added, a key can be queried or
/// removed. This type is the core struct which is wrapped by the cli app.
/// Exmaple usage:
/// ```
/// use kvs::KvStore;
/// let mut kv = KvStore::new();
/// kv.set("one".to_owned(), "number one".to_owned());
/// assert_eq!(kv.get("one".to_owned()), Some("number one".to_owned()));
/// assert_eq!(kv.get("two".to_owned()), None);
/// kv.remove("one".to_owned());
/// assert_eq!(kv.get("one".to_owned()), None);
/// ```
pub struct KvStore {
    kvmap: HashMap<String, u64>,
    kvlog: KvLogStore,
}

impl KvStore {
    /// API to open the KvStore from a given path and return it
    pub fn open<F>(path: F) -> Result<KvStore>
    where
        F: AsRef<Path> + AsRef<OsStr> + Clone,
    {
        let mut kvlog = KvLogStore::new(path)?;
        let kvmap = kvlog.build_map()?;
        Ok(KvStore { kvmap, kvlog })
    }

    /// API to add a key-value pair to the KvStore
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.kvlog.do_compaction(&mut self.kvmap)?;
        let pos = self.kvlog.set(&key, &value)?;
        self.kvmap.insert(key, pos);
        Ok(())
    }

    /// API to query if a key is present in the KvStore and return its value
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(pos) = self.kvmap.get(&key) {
            let value = self.kvlog.get_at_offset(&key, *pos)?;
            return Ok(Some(value));
        }
        Ok(None)
    }

    /// API to remove a key if it exists in the KvStore
    pub fn remove(&mut self, key: String) -> Result<()> {
        self.kvlog.do_compaction(&mut self.kvmap)?;
        if !self.kvmap.contains_key(&key) {
            return Err(err_msg("Key not found"));
        }
        self.kvlog.remove(&key)?;
        self.kvmap.remove(&key);
        Ok(())
    }
}
