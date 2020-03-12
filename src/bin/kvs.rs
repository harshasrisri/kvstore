use clap::{App, SubCommand, Arg};
use kvs::KvStore;

fn main() {
    let args = App::new("Key Value Store")
        .version("0.1.0")
        .subcommand(SubCommand::with_name("set")
                    .arg(
                        Arg::with_name("key")
                        .help("unique key in store")
                        .index(1)
                        .required(true)
                        )
                    .arg(
                        Arg::with_name("value")
                        .help("associated value for key")
                        .index(2)
                        .required(true)
                        )
                    )
        .subcommand(SubCommand::with_name("get")
                    .arg(
                        Arg::with_name("key")
                        .help("unique key in store")
                        .index(1)
                        .required(true)
                        )
                    )
        .subcommand(SubCommand::with_name("rm")
                    .arg(
                        Arg::with_name("key")
                        .help("unique key in store")
                        .index(1)
                        .required(true)
                        )
                    )
        .get_matches();

    let mut kvs = KvStore::new();

    match args.subcommand() {
        ("set", Some(set)) => {
            kvs.set(set.value_of("key").unwrap().to_owned(), set.value_of("value").unwrap().to_owned());
            eprintln!("unimplemented");
            std::process::exit(1)
        }
        ("get", Some(get)) => {
            kvs.get(get.value_of("key").unwrap().to_owned());
            eprintln!("unimplemented");
            std::process::exit(1)
        }
        ("rm", Some(rm)) => {
            kvs.remove(rm.value_of("key").unwrap().to_owned());
            eprintln!("unimplemented");
            std::process::exit(1)
        }
        ("", None) => {
            eprintln!("nothing to do");
            std::process::exit(1);
        }
        _ => unreachable!(),
    }
}
