extern crate syncer;
use std::env;
use std::process;

fn usage() {
  eprintln!("USAGE: syncer <local source> <remote source> <mount point> <max local size in MB>");
  process::exit(2);
}

fn main() {
  let args: Vec<String> = env::args().collect();
  if args.len() != 5 { usage() }

  let source = &args[1];
  let server = &args[2];
  let path = &args[3];
  let maxbytes = match args[4].parse::<u64>() {
    Ok(v) => v * 1000000,
    Err(e) => {
      eprintln!("ERROR: Couldn't understand max local size {:?}: {}", args[4], e);
      usage();
      return
    },
  };
  println!("Starting filesystem from {:?} and {:?} in {:?}", source, server, path);
  match syncer::run(&source, &server, &path, maxbytes) {
    Ok(_) => {},
    Err(e) => eprintln!("FUSE error: {:?}", e),
  }
}
