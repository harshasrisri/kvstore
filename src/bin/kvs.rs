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
            kvs.set(key.clone(), value.clone()).expect(format!("Error adding ({},{})", key, value).as_str());
            eprintln!("unimplemented");
            std::process::exit(1)
        }
        Args::Get{key} => {
            kvs.get(key.clone()).expect(format!("Error getting value for ({})", key).as_str());
            eprintln!("unimplemented");
            std::process::exit(1)
        }
        Args::Rm{key} => {
            kvs.remove(key.clone()).expect(format!("Error removing entry for ({})", key).as_str());
            eprintln!("unimplemented");
            std::process::exit(1)
        }
    }
}
