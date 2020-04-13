use kvs::{KvStore, Operations, Result};
use std::path::PathBuf;
use structopt::StructOpt;
use failure::err_msg;

#[derive(Debug, StructOpt)]
#[structopt(about, author)]
struct Args {
    /// Operations that can be performed on the KvStore
    #[structopt(subcommand)]
    pub ops: Operations,

    /// Path to store the KvStore log file
    #[structopt(short, long, parse(from_os_str), default_value = "KvStore.log")]
    pub log_store: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::from_args();
    let mut kvs = KvStore::open(args.log_store)?;

    match args.ops {
        Operations::Set { key, value } => {
            kvs.set(key, value)?;
        }
        Operations::Get { key } => {
            if let Some(value) = kvs.get(key)? {
                println!("{}", value);
            } else {
                return Err(err_msg("Rm: Key not found"));
            }
        }
        Operations::Rm { key } => {
            kvs.remove(key)?;
        }
    }
    Ok(())
}
