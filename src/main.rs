extern crate syncer;

fn main() {
  let source = "data".to_string();
  let path = "mnt".to_string();
  println!("Starting filesystem from {:?} in {:?}", source, path);
  match syncer::run(&source, &path) {
    Ok(_) => {},
    Err(e) => eprintln!("FUSE error: {:?}", e),
  }
}
