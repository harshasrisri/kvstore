use crate::Result;
use std::fs::{File, OpenOptions};
use std::path::Path;

pub struct KvCli {
    log_handle: File,
}

impl KvCli {
    pub fn new<F>(filename: F) -> Result<KvCli>
    where
        F: AsRef<Path>,
    {
        Ok(KvCli {
            log_handle: OpenOptions::new()
                .read(true)
                .append(true)
                .create(true)
                .open(filename)?,
        })
    }
}
