use kvs::KvStore;
use structopt::StructOpt;

#[derive(Debug,StructOpt)]
#[structopt(about,author)]
enum Args {
    Set {
        /// unique key in store
        #[structopt(required = true)]
        key: String,
        /// associated value for key
        #[structopt(required = true)]
        value: String,
    },
    Get {
        /// unique key in store
        #[structopt(required = true)]
        key: String,
    },
    Rm {
        /// unique key in store
        #[structopt(required = true)]
        key: String,
    }
}

fn main() {
    let args = Args::from_args();
    let mut kvs = KvStore::new();

    match args {
        Args::Set{key, value} => {
            kvs.set(key,value);
            eprintln!("unimplemented");
            std::process::exit(1)
        }
        Args::Get{key} => {
            kvs.get(key);
            eprintln!("unimplemented");
            std::process::exit(1)
        }
        Args::Rm{key} => {
            kvs.remove(key);
            eprintln!("unimplemented");
            std::process::exit(1)
        }
    }
}
