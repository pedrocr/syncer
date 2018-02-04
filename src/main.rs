extern crate syncer;

fn main() {
  let source = "data".to_string();
  let path = "mnt".to_string();
  let server = "localhost:~/blobs/".to_string();
  println!("Starting filesystem from {:?} and {:?} in {:?}", source, server, path);
  match syncer::run(&source, &server, &path) {
    Ok(_) => {},
    Err(e) => eprintln!("FUSE error: {:?}", e),
  }
}
