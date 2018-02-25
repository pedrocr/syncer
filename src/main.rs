extern crate syncer;

use syncer::config;
use std::env;
use std::process;
use std::fs;
use std::path::PathBuf;

fn usage() {
  eprintln!("USAGE:");
  eprintln!("  syncer init <local dir> <remote source> <max local size in MB>");
  eprintln!("  syncer mount <mount dir>");
  process::exit(2);
}

fn main() {
  let args: Vec<String> = env::args().collect();
  match args[1].as_ref() {
    "init"  => init(&args[2..]),
    "mount" => mount(&args[2..]),
    _ => usage(),
  }

}

fn init(args: &[String]) {
  if args.len() != 3 { usage() }

  let mut path = env::current_dir().unwrap();
  path.push(&args[0]);
  let server = args[1].clone();
  let maxbytes = match args[2].parse::<u64>() {
    Ok(v) => v * 1000000,
    Err(e) => {
      eprintln!("ERROR: Couldn't understand max local size {:?}: {}", args[4], e);
      usage();
      return
    },
  };

  let conf = config::Config::new(server, maxbytes);

  match fs::create_dir(&path) {
    Ok(_) => {},
    Err(e) => {eprintln!("Couldn't create dir: {}", e); process::exit(3);},
  }

  let mut conffile = PathBuf::from(&path);
  conffile.push("config");

  match conf.save_config(&conffile) {
    Ok(_) => {},
    Err(e) => {eprintln!("Couldn't save config file: {}", e); process::exit(3);},
  }
}

fn mount(args: &[String]) {
  if args.len() != 2 { usage() }

  let mut path = env::current_dir().unwrap();
  path.push(&args[0]);
  let mut source = path.clone();
  source.push("data");
  let mut config = path.clone();
  config.push("config");
  let mount = PathBuf::from(&args[1]);

  let conf = match config::Config::fetch_config(&config) {
    Ok(c) => c,
    Err(e) => {eprintln!("Couldn't load config file: {}", e); process::exit(3);},
  };

  println!("Starting filesystem from {:?} and {:?} in {:?}", path, conf.server, mount);
  match syncer::run(&source, &mount, &conf) {
    Ok(_) => {},
    Err(e) => eprintln!("MOUNT ERROR: {}", e),
  }
}
