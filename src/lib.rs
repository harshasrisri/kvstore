mod kvcli;
mod kvs;
pub use crate::kvcli::KvCli;
pub use crate::kvs::KvStore;
use serde::{Deserialize, Serialize};
use structopt::StructOpt;

pub type Result<T> = std::result::Result<T, failure::Error>;

#[derive(Debug, StructOpt, Serialize, Deserialize)]
pub enum Operations {
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
