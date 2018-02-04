extern crate rusqlite;
extern crate blake2;
extern crate hex;
extern crate libc;

use super::metadatadb::*;
use self::rusqlite::Connection;
use self::blake2::Blake2b;
use self::blake2::digest::{Input, VariableOutput};
use self::libc::c_int;
use std::cmp;
use std::path::PathBuf;
use std::fs;
use std::io::prelude::*;
use std::usize;
use std::process::Command;
use std::collections::VecDeque;
use std::sync::RwLock;

pub const HASHSIZE: usize = 20;
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
    // As far as I can tell from online references there's no penalty in ext4 for
    // random lookup in a directory with lots of files. So just store all the hashed
    // files in a straight directory with no fanout to not waste space with directory
    // entries. Just doing a 12bit fanout (4096 directories) wastes 17MB on ext4.
    let mut path = path.clone();
    path.push(hex::encode(hash));
    path
  }

  fn get_remote_path(server: &str, hash: &BlobHash) -> String {
    let mut remote = server.to_string();
    remote.push_str(&"/");
    remote.push_str(&hex::encode(hash));
    remote
  }

  fn load(path: &PathBuf, server: &str, hash: &BlobHash) -> Result<Self, c_int> {
    let file = Self::get_path(path, hash);
    if !file.exists() {
      try!(BlobStorage::fetch_from_server(path, server, hash));
    }

    let mut file = match fs::File::open(&file) {
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

  fn store(&self, path: &PathBuf) -> Result<(), c_int> {
    let path = Self::get_path(path, &self.hash);
    if !path.exists() {
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
  server: String,
  metadata: MetadataDB,
  new_blobs: RwLock<VecDeque<BlobHash>>,
}

impl BlobStorage {
  pub fn new(source: &str, server: &str) -> Result<Self, c_int> {
    let mut path = PathBuf::from(source);
    path.push("blobs");
    match fs::create_dir_all(&path) {
      Ok(_) => {},
      Err(_) => return Err(libc::EIO),
    }

    // Create the db file to pass to MetadataDB
    let mut file = PathBuf::from(source);
    file.push("metadata.sqlite3");
    let connection = Connection::open(&file).unwrap();

    Ok(BlobStorage {
      source: path,
      server: server.to_string(),
      metadata: MetadataDB::new(connection),
      new_blobs: RwLock::new(VecDeque::new()),
    })
  }

  pub fn read_all(&self, hash: &BlobHash) -> Result<Vec<u8>, c_int> {
    self.read(hash, 0, usize::MAX)
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
    self.metadata.touch_blob(hash);
    Blob::load(&self.source, &self.server, hash)
  }

  fn store_blob(&self, blob: Blob) -> Result<(), c_int> {
    try!(blob.store(&self.source));
    try!(self.metadata.set_blob(&blob.hash, blob.data.len() as u64));
    let mut new_blobs = self.new_blobs.write().unwrap();
    new_blobs.push_back(blob.hash);
    Ok(())
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

  pub fn add_node(&self, node: u64, data: &[u8]) -> Result<BlobHash, c_int> {
    let hash = try!(self.add_blob(data));
    try!(self.metadata.set_node(node, &hash));
    Ok(hash)
  }

  pub fn read_node(&self, node: u64) -> Result<Vec<u8>, c_int> {
    self.read_all(&try!(self.metadata.get_node(node)))
  }

  fn upload_to_server(path: &PathBuf, server: &str, hash: &BlobHash) -> Result<(), c_int> {
    let path = Blob::get_path(path, hash);
    if !path.exists() {
      return Err(libc::EIO);
    }
    for _ in 0..10 {
      match Command::new("rsync").arg("--timeout=5").arg(&path).arg(server).status() {
        Ok(_) => return Ok(()),
        Err(_) => {},
      }
    }
    eprintln!("Failed to upload block to server");
    Err(libc::EIO)
  }

  fn fetch_from_server(path: &PathBuf, server: &str, hash: &BlobHash) -> Result<(), c_int> {
    let remote = Blob::get_remote_path(server, hash);
    for _ in 0..10 {
      match Command::new("rsync").arg("--timeout=5").arg(&remote).arg(&path).status() {
        Ok(_) => return Ok(()),
        Err(_) => {},
      }
    }
    eprintln!("Failed to get block from server");
    Err(libc::EIO)
  }

  pub fn do_uploads(&self) {
    let count = {
      let new_blobs = self.new_blobs.read().unwrap();
      new_blobs.len()
    };
    for _ in 0..count {
      let hash = {
        let new_blobs = self.new_blobs.read().unwrap();
        new_blobs.front().unwrap().clone()
      };
      match Self::upload_to_server(&self.source, &self.server, &hash) {
        Ok(_) => {
          // sync worked so this hash is no longer needed
          { 
            // get the hash out of the list in a block to hold the lock for as short as possible
            let mut new_blobs = self.new_blobs.write().unwrap();
            new_blobs.pop_front();
          }
          // Mark the block as already synced
          self.metadata.mark_synced_blob(&hash);
        },
        Err(_) => {},
      }
    }
  }
}
