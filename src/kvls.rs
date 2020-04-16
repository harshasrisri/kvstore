use crate::{Operations, Result};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, BufReader, BufWriter};
use std::path::Path;
use failure::err_msg;

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
    pub fn set(&mut self, key: &str, value: &str) -> Result<u64> {
        let op = Operations::Set {
            key: key.to_owned(),
            value: value.to_owned(),
        };
        let pos = self.log_writer.seek(SeekFrom::End(0))?;
        serde_json::to_writer(&mut self.log_writer, &op)?;
        self.log_writer.seek(SeekFrom::End(0))?;
        Ok(pos)
    }

    /// API to remove a key if it exists in the Kv Log Store
    pub fn remove(&mut self, key: &str) -> Result<()> {
        let op = Operations::Rm {
            key: key.to_owned(),
        };
        serde_json::to_writer(&mut self.log_writer, &op)?;
        Ok(())
    }

    pub fn to_map(&mut self) -> Result<HashMap<String, u64>> {
        let reader = self.log_reader.get_mut();
        let mut map = HashMap::new();
        let mut pos = reader.seek(SeekFrom::Start(0))?;
        let mut stream = serde_json::Deserializer::from_reader(reader).into_iter();
        while let Some(op) = stream.next() {
            match op? {
                Operations::Set { key, value: _ } => map.insert(key, pos),
                Operations::Rm { key } => map.remove(&key),
                Operations::Get { .. } => None,
            };
            pos = stream.byte_offset() as u64;
        }
        Ok(map)
    }

    pub fn get_at_offset(&mut self, lookup_key: &str, pos: u64) -> Result<String> {
        let reader = self.log_reader.get_mut();
        reader.seek(SeekFrom::Start(pos))?;
        let mut stream = serde_json::Deserializer::from_reader(reader).into_iter();
        while let Some(op) = stream.next() {
            if let Operations::Set { key, value } = op? {
                if lookup_key == key {
                    return Ok(value);
                }
            }
            break;
        }
        return Err(err_msg("Key not found"));
    }
}
