use kvs::{KvStore, Operations, Result};
use std::path::PathBuf;
use structopt::StructOpt;

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
            kvs.get(key)?;
            eprintln!("unimplemented");
            std::process::exit(1)
        }
        Operations::Rm { key } => {
            kvs.remove(key)?;
            eprintln!("unimplemented");
            std::process::exit(1)
        }
    }
    Ok(())
}
