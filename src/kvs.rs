use std::collections::HashMap;
use std::path::PathBuf;

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
#[derive(Default)]
pub struct KvStore {
    store: HashMap<String, String>,
}

impl KvStore {
    /// Method to create a new and empty KvStore
    pub fn new() -> KvStore {
        KvStore {
            store: HashMap::new(),
        }
    }

    /// API to add a key-value pair to the KvStore
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.store.insert(key, value);
        Ok(())
    }

    /// API to query if a key is present in the KvStore and return its value
    pub fn get(&self, key: String) -> Result<Option<String>> {
        Ok(self.store.get(&key).cloned())
    }

    /// API to remove a key if it exists in the KvStore
    pub fn remove(&mut self, key: String) -> Result<()> {
        self.store.remove(&key);
        Ok(())
    }

    /// API to open the KvStore from a given path and return it
    pub fn open(_path: impl Into<PathBuf>) -> Result<KvStore> {
        Ok(KvStore::new())
    }
}

pub type Result<T> = std::result::Result<T, std::io::Error>;
