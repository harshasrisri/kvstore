mod kvs;
pub use crate::kvs::{KvStore, Result};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
pub enum Ops {
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
    },
}

