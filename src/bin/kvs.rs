use kvs::{KvStore, Operations, Result};
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(about, author)]
struct Args {
    /// Operations that can be performed on the KvStore
    #[structopt(subcommand)]
    pub ops: Operations,

    /// Path where the KvStore.log file is present
    #[structopt(short, long, parse(from_os_str), default_value = ".")]
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
                println!("Key not found");
            }
        }
        Operations::Rm { key } => {
            if kvs.remove(key).is_err() {
                println!("Key not found");
                std::process::exit(1);
            }
        }
    }
    Ok(())
}
