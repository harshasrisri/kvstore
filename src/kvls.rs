use crate::{Operations, Result};
use failure::err_msg;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, SeekFrom};
use std::path::Path;

pub struct KvLogStore {
    log_reader: BufReader<File>,
    log_writer: BufWriter<File>,
    uniq_keys: usize,
    entries: usize,
    mapped: bool,
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
            uniq_keys: 0,
            entries: 0,
            mapped: false,
        })
    }

    /// API to add a key-value pair to the Kv Log Store
    pub fn set(&mut self, key: &str, value: &str) -> Result<u64> {
        self.is_mapped()?;
        let op = Operations::Set {
            key: key.to_owned(),
            value: value.to_owned(),
        };
        let pos = self.log_writer.seek(SeekFrom::End(0))?;
        serde_json::to_writer(&mut self.log_writer, &op)?;
        self.log_writer.seek(SeekFrom::End(0))?;
        self.entries += 1;
        self.uniq_keys += 1;
        Ok(pos)
    }

    /// API to remove a key if it exists in the Kv Log Store
    pub fn remove(&mut self, key: &str) -> Result<()> {
        self.is_mapped()?;
        let op = Operations::Rm {
            key: key.to_owned(),
        };
        serde_json::to_writer(&mut self.log_writer, &op)?;
        self.entries += 1;
        self.uniq_keys -= 1;
        Ok(())
    }

    pub fn build_map(&mut self) -> Result<HashMap<String, u64>> {
        let reader = self.log_reader.get_mut();
        let mut map = HashMap::new();
        let mut pos = reader.seek(SeekFrom::Start(0))?;
        let mut stream = serde_json::Deserializer::from_reader(reader).into_iter();
        while let Some(op) = stream.next() {
            match op? {
                Operations::Set { key, .. } => {
                    map.insert(key, pos);
                    self.uniq_keys += 1;
                }
                Operations::Rm { key } => {
                    map.remove(&key);
                    self.uniq_keys -= 1;
                }
                Operations::Get { .. } => {
                    panic!("Not supposed to encounter a Get entry");
                }
            };
            pos = stream.byte_offset() as u64;
            self.entries += 1;
        }
        self.mapped = true;
        Ok(map)
    }

    pub fn get_at_offset(&mut self, lookup_key: &str, pos: u64) -> Result<String> {
        self.is_mapped()?;
        let reader = self.log_reader.get_mut();
        reader.seek(SeekFrom::Start(pos))?;
        let stream = serde_json::Deserializer::from_reader(reader).into_iter();
        for op in stream {
            if let Operations::Set { key, value } = op? {
                if lookup_key == key {
                    return Ok(value);
                }
            }
            break;
        }
        Err(err_msg("Key not found"))
    }

    fn is_mapped(&self) -> Result<()> {
        if self.mapped {
            Ok(())
        } else {
            Err(err_msg("Log file not yet mapped"))
        }
    }
}
