use crate::{Operations, Result};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter};
use std::path::Path;

pub struct KvLogStore {
    log_reader: BufReader<File>,
    log_writer: BufWriter<File>,
}

impl KvLogStore {
    /// Method to open a Key Value Store from a file
    pub fn new<F>(filename: F) -> Result<KvLogStore>
    where
        F: AsRef<Path> + Clone,
    {
        let log_handle = OpenOptions::new()
            .create(true)
            .append(true)
            .open(filename.clone())?;

        let log_writer = BufWriter::new(log_handle);
        let log_reader = BufReader::new(File::open(filename)?);

        Ok(KvLogStore {
            log_reader,
            log_writer,
        })
    }

    /// API to add a key-value pair to the Kv Log Store
    pub fn set(&mut self, key: &str, value: &str) -> Result<()> {
        let op = Operations::Set {
            key: key.to_owned(),
            value: value.to_owned(),
        };
        serde_json::to_writer(&mut self.log_writer, &op)?;
        Ok(())
    }

    /// API to remove a key if it exists in the Kv Log Store
    pub fn remove(&mut self, key: &str) -> Result<()> {
        let op = Operations::Rm {
            key: key.to_owned(),
        };
        serde_json::to_writer(&mut self.log_writer, &op)?;
        Ok(())
    }

    pub fn to_map(&self) -> Result<HashMap<String, String>> {
        let reader = self.log_reader.get_ref().clone();
        let mut map = HashMap::new();
        let stream = serde_json::Deserializer::from_reader(reader).into_iter::<Operations>();
        for op in stream {
            match op? {
                Operations::Set { key, value } => map.insert(key, value),
                Operations::Rm { key } => map.remove(&key),
                Operations::Get { key } => None,
            };
        }
        Ok(map)
    }
}
