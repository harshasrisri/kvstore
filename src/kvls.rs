use crate::{Operations, Result};
use failure::err_msg;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, SeekFrom};
use std::path::{Path, PathBuf};

pub struct KvLogStore {
    path: PathBuf,
    reader: BufReader<File>,
    writer: BufWriter<File>,
    mapped: bool,
    compaction_size: usize,
}

const LOG_FILE_NAME: &str = "kvls.ser";
const COMPACT_FILE_NAME: &str = "kvls.compact.ser";

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
            path,
            reader,
            writer,
            mapped: false,
            compaction_size: 1024,
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

    /// API to add a key-value pair to the Kv Log Store
    pub fn set(&mut self, key: &str, value: &str) -> Result<u64> {
        self.checks_and_balances()?;
        let op = Operations::Set {
            key: key.to_owned(),
            value: value.to_owned(),
        };
        let pos = self.writer.seek(SeekFrom::End(0))?;
        serde_json::to_writer(&mut self.writer, &op)?;
        self.writer.seek(SeekFrom::End(0))?;
        Ok(pos)
    }

    /// API to remove a key if it exists in the Kv Log Store
    pub fn remove(&mut self, key: &str) -> Result<()> {
        self.checks_and_balances()?;
        let op = Operations::Rm {
            key: key.to_owned(),
        };
        serde_json::to_writer(&mut self.writer, &op)?;
        Ok(())
    }

    pub fn build_map(&mut self) -> Result<HashMap<String, u64>> {
        let reader = self.reader.get_mut();
        let mut map = HashMap::new();
        let mut pos = reader.seek(SeekFrom::Start(0))?;
        let mut stream = serde_json::Deserializer::from_reader(reader).into_iter();
        while let Some(op) = stream.next() {
            match op? {
                Operations::Set { key, .. } => {
                    map.insert(key, pos);
                }
                Operations::Rm { key } => {
                    map.remove(&key);
                }
                Operations::Get { .. } => {
                    panic!("Not supposed to encounter a Get entry");
                }
            };
            pos = stream.byte_offset() as u64;
        }
        self.mapped = true;
        Ok(map)
    }

    pub fn get_at_offset(&mut self, lookup_key: &str, pos: u64) -> Result<String> {
        let reader = self.reader.get_mut();
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

    fn checks_and_balances(&mut self) -> Result<()> {
        if !self.mapped {
            Err(err_msg("Log file not yet mapped"))
        } else if self.writer.seek(SeekFrom::Current(0))? < self.compaction_size as u64 {
            Ok(())
        } else {
            self.compact()
        }
    }

    fn compact(&mut self) -> Result<()> {
        let compact_file = Path::new(&self.path).join(COMPACT_FILE_NAME);
        let mut writer = BufWriter::new(File::create(&compact_file)?);

        for (key, pos) in self.build_map()? {
            let value = self.get_at_offset(&key, pos)?;
            let op = Operations::Set { key, value };
            serde_json::to_writer(&mut writer, &op)?;
        }

        let log_file = Path::new(&self.path).join(LOG_FILE_NAME);
        std::fs::remove_file(&log_file)?;
        std::fs::rename(compact_file, log_file)?;

        let (reader, writer) = Self::open_file_handles(&self.path)?;
        self.reader = reader;
        self.writer = writer;

        if self.writer.seek(SeekFrom::Current(0))? > self.compaction_size as u64 {
            self.compaction_size *= 2;
        }

        Ok(())
    }
}
