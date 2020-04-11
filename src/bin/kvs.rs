use kvs::{KvStore, Ops, Result};
use structopt::StructOpt;
use std::path::PathBuf;

#[derive(Debug, StructOpt)]
#[structopt(about, author)]
struct Args {
    /// Operations that can be performed on the KvStore
    #[structopt(subcommand)]
    pub ops: Ops,

    /// Path to store the KvStore log file
    #[structopt(short, long, parse(from_os_str), default_value = "KvStore.log")]
    pub log_store: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::from_args();
    let mut kvs = KvStore::new(args.log_store)?;

    match args.ops {
        Ops::Set { key, value } => {
            kvs.set(key, value)?;
            eprintln!("unimplemented");
            std::process::exit(1)
        }
        Ops::Get { key } => {
            kvs.get(key)?;
            eprintln!("unimplemented");
            std::process::exit(1)
        }
        Ops::Rm { key } => {
            kvs.remove(key)?;
            eprintln!("unimplemented");
            std::process::exit(1)
        }
    }
}
