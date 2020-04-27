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
    num_entries: usize,
    max_entries: usize,
}

#[derive(Serialize)]
struct SeLogEntry<'a> {
    key: &'a str,
    value: Option<&'a str>,
}

#[derive(Deserialize)]
struct DeLogEntry {
    key: String,
    value: Option<String>,
}

const LOG_FILE_NAME: &str = "kvls.ser";
const COMPACTION_FILE: &str = "kvls.compact.ser";

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
            num_entries: 0,
            max_entries: 1024,
        })
    }

    fn open_file_handles<F>(path: F, file: &str) -> Result<(BufReader<File>, BufWriter<File>)>
    where
        F: AsRef<Path> + AsRef<OsStr>,
    {
        let filename = Path::new(&path).join(file);
        let log_handle = OpenOptions::new()
            .create(true)
            .append(true)
            .open(filename.clone())?;

        let writer = BufWriter::new(log_handle);
        let reader = BufReader::new(File::open(filename)?);

        Ok((reader, writer))
    }

    fn commit_operation(op: SeLogEntry, mut writer: impl Write + Seek) -> Result<u64> {
        let v = serde_json::to_vec(&op)?;
        writer.write_all(&v)?;
        let end = writer.seek(SeekFrom::End(0))?;
        Ok(end - v.len() as u64)
    }

    /// API to add a key-value pair to the Kv Log Store
    pub fn set(&mut self, key: &str, value: &str) -> Result<u64> {
        let entry = SeLogEntry {
            key,
            value: Some(value),
        };
        let pos = Self::commit_operation(entry, &mut self.writer)?;
        self.num_entries += 1;
        Ok(pos)
    }

    /// API to remove a key if it exists in the Kv Log Store
    pub fn remove(&mut self, key: &str) -> Result<()> {
        let entry = SeLogEntry { key, value: None };
        Self::commit_operation(entry, &mut self.writer)?;
        self.num_entries += 1;
        Ok(())
    }

    pub fn build_map(&mut self) -> Result<HashMap<String, u64>> {
        let reader = self.reader.get_mut();
        let mut map = HashMap::new();
        let mut pos = reader.seek(SeekFrom::Start(0))?;
        let mut stream = serde_json::Deserializer::from_reader(reader).into_iter();
        while let Some(op) = stream.next() {
            match op? {
                DeLogEntry {
                    key,
                    value: Some(_),
                } => {
                    map.insert(key, pos);
                }
                DeLogEntry { key, value: None } => {
                    map.remove(&key);
                }
            };
            self.num_entries += 1;
            pos = stream.byte_offset() as u64;
        }
        Ok(map)
    }

    pub fn get_at_offset(&self, key: &str, pos: u64) -> Result<String> {
        let mut reader = self.reader.get_ref().try_clone()?;
        reader.seek(SeekFrom::Start(pos))?;
        let stream = serde_json::Deserializer::from_reader(reader).into_iter::<DeLogEntry>();
        for op in stream {
            let op = op?;
            if op.key == key {
                if let Some(value) = op.value {
                    return Ok(value);
                } else {
                    return Err(err_msg("KV map out of sync with KV store"));
                }
            } else {
                return Err(err_msg(format!(
                    "Key mismatch in log store. Expected: {}. Found: {}",
                    op.key, key
                )));
            }
        }
        panic!("Shouldn't have been here!")
    }

    fn compaction_analysis(&mut self, map_len: usize) -> bool {
        if self.num_entries < self.max_entries {
            return false;
        } else if self.num_entries < 2 * map_len {
            while self.num_entries > self.max_entries {
                self.max_entries *= 2;
            }
            return false;
        }
        return true;
    }

    pub fn do_compaction(&mut self, map: &mut HashMap<String, u64>) -> Result<bool> {
        if !self.compaction_analysis(map.len()) {
            return Ok(false);
        }

        let start = std::time::Instant::now();
        eprintln!("Num Entries Before Compaction : {}", self.num_entries);

        let (_reader, mut writer) = Self::open_file_handles(&self.path, COMPACTION_FILE)?;

        for (key, pos) in map.iter_mut() {
            let value = self.get_at_offset(key, *pos)?;
            let entry = SeLogEntry {
                key,
                value: Some(&value),
            };
            *pos = Self::commit_operation(entry, &mut writer)?;
        }

        std::fs::rename(
            self.path.join(COMPACTION_FILE),
            self.path.join(LOG_FILE_NAME),
        )?;

        let (reader, writer) = Self::open_file_handles(&self.path, LOG_FILE_NAME)?;
        self.reader = reader;
        self.writer = writer;
        self.num_entries = map.len();

        eprintln!(
            "Num Entries After  Compaction : {}. Time taken: {}ms",
            self.num_entries,
            start.elapsed().as_millis()
        );
        Ok(true)
    }
}
