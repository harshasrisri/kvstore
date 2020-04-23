use crate::Result;
use failure::err_msg;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub struct KvLogStore {
    reader: BufReader<File>,
    writer: BufWriter<File>,
}

#[derive(Serialize)]
struct SerLogEntry<'a, K, V> {
    key: &'a K,
    value: Option<&'a V>,
}

#[derive(Deserialize)]
struct DeLogEntry<K, V> {
    key: K,
    value: Option<V>,
}

const LOG_FILE_NAME: &str = "kvls.ser";

impl KvLogStore {
    /// Method to open a Key Value Store from a file
    pub fn new<F>(path: F) -> Result<KvLogStore>
    where
        F: AsRef<Path> + AsRef<OsStr> + Clone,
    {
        let path = Path::new(&path).to_path_buf();
        if !path.exists() || !path.is_dir() {
            return Err(err_msg("Error processing path"));
        }

        let (reader, writer) = Self::open_file_handles(&path)?;

        Ok(KvLogStore {
            reader,
            writer,
        })
    }

    fn open_file_handles(path: &PathBuf) -> Result<(BufReader<File>, BufWriter<File>)> {
        let filename = Path::new(path).join(LOG_FILE_NAME);
        let log_handle = OpenOptions::new()
            .create(true)
            .append(true)
            .open(filename.clone())?;

        let writer = BufWriter::new(log_handle);
        let reader = BufReader::new(File::open(filename)?);

        Ok((reader, writer))
    }

    fn commit_operation<K, V>(op: &SerLogEntry<K, V>, mut writer: impl Write + Seek) -> Result<u64> 
    where 
        K: Serialize,
        V: Serialize,
    {
        let v = serde_json::to_vec(op)?;
        writer.write_all(&v)?;
        let end = writer.seek(SeekFrom::End(0))?;
        Ok(end - v.len() as u64)
    }

    /// API to add a key-value pair to the Kv Log Store
    pub fn set<K,V>(&mut self, key: &K, value: &V) -> Result<u64> 
    where 
        K: Serialize,
        V: Serialize,
    {
        let entry = SerLogEntry {
            key: key.to_owned(),
            value: Some(value.to_owned()),
        };
        Self::commit_operation(&entry, &mut self.writer)
    }

    /// API to remove a key if it exists in the Kv Log Store
    pub fn remove<K: Serialize>(&mut self, key: &K) -> Result<()> {
        let entry: SerLogEntry<K, K> = SerLogEntry {
            key,
            value: None,
        };
        Self::commit_operation(&entry, &mut self.writer)?;
        Ok(())
    }

    pub fn build_map<'de, K, V>(&mut self) -> Result<HashMap<K, u64>> 
    where
    K: std::cmp::Eq + std::hash::Hash + Deserialize<'de>,
    V: Deserialize<'de>,
    {
        let reader = self.reader.get_mut();
        let mut map = HashMap::new();
        let mut pos = reader.seek(SeekFrom::Start(0))?;
        let mut stream = serde_json::Deserializer::from_reader(reader).into_iter();
        while let Some(op) = stream.next() {
            let entry: DeLogEntry<K,V> = op?;
            if entry.value.is_some() {
                map.insert(entry.key, pos);
            } else {
                map.remove(&entry.key);
            }
            pos = stream.byte_offset() as u64;
        }
        Ok(map)
    }

    pub fn get_at_offset<'de, K, V>(&self, lookup_key: &K, pos: u64) -> Result<V> 
    where
        K: Deserialize<'de> + std::cmp::PartialEq,
        V: Deserialize<'de>,
    {
        let mut reader = self.reader.get_ref().try_clone()?;
        reader.seek(SeekFrom::Start(pos))?;
        let stream = serde_json::Deserializer::from_reader(reader).into_iter();
        for op in stream {
            let op: DeLogEntry<K, V> = op?;
            if op.key == *lookup_key {
                if let Some(value) = op.value {
                    return Ok(value);
                } else {
                    return Err(err_msg("KV map out of sync with KV store"));
                }
            } else {
                return Err(err_msg(format!(
                    "Key mismatch in log store")));
            }
        }
        panic!("Shouldn't have been here!")
    }
}
