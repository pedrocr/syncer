extern crate syncer;

fn main() {
  let path = "mnt".to_string();
  println!("Starting filesystem in {:?}", path);
  match syncer::run(&path) {
    Ok(_) => {},
    Err(e) => eprintln!("FUSE error: {:?}", e),
  }
}
