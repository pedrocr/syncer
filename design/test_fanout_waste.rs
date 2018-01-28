use std::path::PathBuf;
use std::fs;

fn main() {
  let path = PathBuf::from("data");
  for i in 0..4096 {
    println!("at {}", i);
    let mut dir = path.clone();
    dir.push(format!("{:03x}",i));
    fs::create_dir_all(&dir);
  }
}
