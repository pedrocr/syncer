use std::cmp;
extern crate blake2;
use self::blake2::Blake2b;
use self::blake2::digest::{Input, VariableOutput};
extern crate hex;
extern crate libc;
use self::libc::c_int;
use std::path::PathBuf;
use std::fs;
use std::io::prelude::*;

const HASHSIZE: usize = 20;
pub type BlobHash = [u8;HASHSIZE];

#[derive(Clone)]
struct Blob {
  data: Vec<u8>,
  hash: BlobHash,
}

impl Blob {
  pub fn zero(size: usize) -> Self {
    Self::new_with_data(vec![0 as u8; size])
  }

  pub fn new_with_data(data: Vec<u8>) -> Self {
    let hash = Self::hash(&data);
    Self {
      data,
      hash,
    }
  }

  fn get_path(path: &PathBuf, hash: &BlobHash) -> PathBuf {
    let mut path = path.clone();
    // 6 bytes of fanout should allow >20TB easily
    let filename = hex::encode(hash);
    path.push(&filename[0..3]);
    path.push(&filename[3..6]);
    path.push(filename);
    path
  }

  fn load(path: &PathBuf, hash: &BlobHash) -> Result<Self, c_int> {
    let path = Self::get_path(path, hash);
    if !path.exists() {
      Err(libc::EIO)
    } else {
      let mut file = match fs::File::open(&path) {
        Ok(f) => f,
        Err(_) => return Err(libc::EIO),
      };
      let mut buffer = Vec::new();
      match file.read_to_end(&mut buffer) {
        Ok(_) => {},
        Err(_) => return Err(libc::EIO),
      }
      Ok(Self::new_with_data(buffer))
    }
  }

  fn store(&self, path: &PathBuf) -> Result<(), c_int> {
    let path = Self::get_path(path, &self.hash);
    if !path.exists() {
      let dir = path.parent().unwrap();
      match fs::create_dir_all(&dir) {
        Ok(_) => {},
        Err(_) => return Err(libc::EIO),
      }
      let mut file = match fs::File::create(&path) {
        Ok(f) => f,
        Err(_) => return Err(libc::EIO),
      };
      match file.write_all(&self.data) {
        Ok(_) => {},
        Err(_) => return Err(libc::EIO),
      }
    }
    Ok(())
  }

  fn read(&self, offset: usize, bytes: usize) -> Vec<u8> {
    assert!(offset < self.data.len());
    let start = offset;
    let end = cmp::min(offset+bytes, self.data.len());
    self.data[start..end].to_vec()
  }

  fn write(&self, offset: usize, data: &[u8]) -> Blob {
    let start = offset;
    let end = cmp::min(offset+data.len(), self.data.len());
    let bytes = end - start;
    let mut newdata = self.data.clone();
    newdata[start..end].copy_from_slice(&data[0..bytes]);
    Self::new_with_data(newdata)
  }

  fn hash(data: &[u8]) -> BlobHash {
    let mut hasher = Blake2b::new(HASHSIZE).unwrap();
    hasher.process(data);
    let mut buf = [0u8; HASHSIZE];
    hasher.variable_result(&mut buf).unwrap();
    buf
  }
}

pub struct BlobStorage {
  source: PathBuf,
}

impl BlobStorage {
  pub fn new(source: &str) -> Self {
    BlobStorage {
      source: PathBuf::from(source),
    }
  }

  pub fn read(&self, hash: &BlobHash, offset: usize, bytes: usize) -> Result<Vec<u8>, c_int> {
    let blob = try!(self.get_blob(hash));
    Ok(blob.read(offset, bytes))
  }

  pub fn write(&self, hash: &BlobHash, offset: usize, data: &[u8]) -> Result<BlobHash, c_int> {
    let blob = try!(self.get_blob(hash));
    let new_blob = blob.write(offset, data);
    let hash = new_blob.hash;
    try!(self.store_blob(new_blob));
    Ok(hash)
  }

  fn get_blob(&self, hash: &BlobHash) -> Result<Blob, c_int> {
    Blob::load(&self.source, hash)
  }

  fn store_blob(&self, blob: Blob) -> Result<(), c_int> {
    blob.store(&self.source)
  }

  pub fn zero(size: usize) -> BlobHash {
    Blob::zero(size).hash
  }

  pub fn add_blob(&self, data: &[u8]) -> Result<BlobHash, c_int> {
    let blob = Blob::new_with_data(data.to_vec());
    let hash = blob.hash;
    try!(self.store_blob(blob));
    Ok(hash)
  }
}
