use crate::Result;
use std::fs::{File, OpenOptions};
use std::path::Path;

pub struct KvLogStore {
    log_handle: File,
}

impl KvLogStore {
    /// Method to open a Key Value Store from a file
    pub fn new<F>(filename: F) -> Result<KvLogStore>
    where
        F: AsRef<Path>,
    {
        Ok(KvLogStore {
            log_handle: OpenOptions::new()
                .read(true)
                .append(true)
                .create(true)
                .open(filename)?,
        })
    }

    /// API to add a key-value pair to the Kv Log Store
    pub fn set(&mut self, _key: &String, _value: &String) -> Result<()> {
        unimplemented!()
    }

    /// API to remove a key if it exists in the Kv Log Store
    pub fn remove(&mut self, _key: &String) -> Result<()> {
        unimplemented!()
    }
}
