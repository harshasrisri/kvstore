use crate::Result;
use failure::err_msg;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub struct KvLogStore {
    path: PathBuf,
    reader: BufReader<File>,
    writer: BufWriter<File>,
    entries: usize,
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
const COMPACTED_FILE: &str = "kvls.compacted.ser";

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

        let (reader, writer) = Self::open_file_handles(&path, LOG_FILE_NAME)?;

        Ok(KvLogStore {
            path,
            reader,
            writer,
            entries: 0,
        })
    }

    fn open_file_handles(path: &PathBuf, file: &str) -> Result<(BufReader<File>, BufWriter<File>)> {
        let filename = Path::new(path).join(file);
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
            key: key,
            value: Some(value),
        };
        let pos = Self::commit_operation(&entry, &mut self.writer)?;
        self.entries += 1;
        Ok(pos)
    }

    /// API to remove a key if it exists in the Kv Log Store
    pub fn remove<K: Serialize>(&mut self, key: &K) -> Result<()> {
        let entry: SerLogEntry<K, K> = SerLogEntry {
            key,
            value: None,
        };
        Self::commit_operation(&entry, &mut self.writer)?;
        self.entries += 1;
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
            self.entries += 1;
            pos = stream.byte_offset() as u64;
        }
        Ok(map)
    }

    pub fn get_at_offset<'de, K, V>(&self, key: &K, pos: u64) -> Result<V> 
    where
        K: Deserialize<'de> + std::cmp::PartialEq,
        V: Deserialize<'de>,
    {
        let mut reader = self.reader.get_ref().try_clone()?;
        reader.seek(SeekFrom::Start(pos))?;
        let stream = serde_json::Deserializer::from_reader(reader).into_iter();
        for op in stream {
            let op: DeLogEntry<K, V> = op?;
            if op.key == *key {
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

    fn needs_compaction(&self) -> bool {
        static mut LIMIT: usize = 1024;
        unsafe {
            if self.entries >= LIMIT {
                LIMIT *= 2;
                true
            } else {
                false
            }
        }
    }

    pub fn do_compaction<'de, K>(&mut self, map: &mut HashMap<K, u64>) -> Result<bool>
    where
    K: std::fmt::Debug + std::cmp::Eq + std::hash::Hash + Serialize + Deserialize<'de>,
    {
        if !self.needs_compaction() {
            return Ok(false);
        }

        let (_reader, mut writer) = Self::open_file_handles(&self.path, COMPACTED_FILE)?;

        for (key, pos) in map.iter_mut() {
            eprintln!("key: {:?}, pos: {}", key, pos);
            let value = self.get_at_offset(key, *pos)?;
            eprintln!("value: {:?}", value);
            let entry = SerLogEntry {
                key: &key, value: Some(&value),
            };
            *pos = Self::commit_operation(&entry, &mut writer)?;
        }

        std::fs::rename(&self.path.join(COMPACTED_FILE), &self.path.join(LOG_FILE_NAME))?;
        let (reader, writer) = Self::open_file_handles(&self.path, LOG_FILE_NAME)?;
        self.reader = reader;
        self.writer = writer;

        Ok(true)
    }
}
