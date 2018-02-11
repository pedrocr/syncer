extern crate syncer;
use std::env;
use std::process;

fn usage() {
  eprintln!("USAGE: syncer <local source> <remote source> <mount point>");
  process::exit(2);
}

fn main() {
  let args: Vec<String> = env::args().collect();
  if args.len() != 4 { usage() }

  let source = &args[1];
  let server = &args[2];
  let path = &args[3];
  println!("Starting filesystem from {:?} and {:?} in {:?}", source, server, path);
  match syncer::run(&source, &server, &path) {
    Ok(_) => {},
    Err(e) => eprintln!("FUSE error: {:?}", e),
  }
}
