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
    mapped: bool,
}

#[derive(Serialize, Deserialize)]
struct LogEntry {
    key: String,
    value: Option<String>,
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
            mapped: false,
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

    fn commit_operation(op: &LogEntry, mut writer: impl Write + Seek) -> Result<u64> {
        let v = serde_json::to_vec(op)?;
        writer.write_all(&v)?;
        let end = writer.seek(SeekFrom::End(0))?;
        Ok(end - v.len() as u64)
    }

    /// API to add a key-value pair to the Kv Log Store
    pub fn set(&mut self, key: &str, value: &str) -> Result<u64> {
        let entry = LogEntry {
            key: key.to_owned(),
            value: Some(value.to_owned()),
        };
        Self::commit_operation(&entry, &mut self.writer)
    }

    /// API to remove a key if it exists in the Kv Log Store
    pub fn remove(&mut self, key: &str) -> Result<()> {
        let entry = LogEntry {
            key: key.to_owned(),
            value: None,
        };
        Self::commit_operation(&entry, &mut self.writer)?;
        Ok(())
    }

    pub fn build_map(&mut self) -> Result<HashMap<String, u64>> {
        let reader = self.reader.get_mut();
        let mut map = HashMap::new();
        let mut pos = reader.seek(SeekFrom::Start(0))?;
        let mut stream = serde_json::Deserializer::from_reader(reader).into_iter();
        while let Some(op) = stream.next() {
            match op? {
                LogEntry {
                    key,
                    value: Some(_),
                } => {
                    map.insert(key, pos);
                }
                LogEntry { key, value: None } => {
                    map.remove(&key);
                }
            };
            pos = stream.byte_offset() as u64;
        }
        self.mapped = true;
        Ok(map)
    }

    pub fn get_at_offset(&self, lookup_key: &str, pos: u64) -> Result<String> {
        let mut reader = self.reader.get_ref().try_clone()?;
        reader.seek(SeekFrom::Start(pos))?;
        let stream = serde_json::Deserializer::from_reader(reader).into_iter::<LogEntry>();
        for op in stream {
            let op = op?;
            if op.key == lookup_key {
                if let Some(value) = op.value {
                    return Ok(value);
                } else {
                    return Err(err_msg("KV map out of sync with KV store"));
                }
            } else {
                return Err(err_msg(format!(
                    "Key mismatch in log store. Expected: {}. Found: {}",
                    op.key, lookup_key
                )));
            }
        }
        panic!("Shouldn't have been here!")
    }
}
