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
use std::path::{Path, PathBuf};
use std::fs;
use std::io::prelude::*;
use std::usize;
use std::process::Command;
use std::collections::{VecDeque,HashMap};
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

  fn load(file: &Path) -> Result<Self, c_int> {
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

  fn store(&self, file: &Path) -> Result<(), c_int> {
    if !file.exists() {
      let mut file = match fs::File::create(&file) {
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
  maxbytes: u64,
  localbytes: RwLock<u64>,
  source: PathBuf,
  server: String,
  metadata: MetadataDB,
  new_blobs: RwLock<VecDeque<BlobHash>>,
  touched_blobs: RwLock<HashMap<BlobHash,i64>>,
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
    let meta = MetadataDB::new(connection);
    let to_upload = meta.to_upload();
    let localbytes = meta.localbytes();

    Ok(BlobStorage {
      maxbytes: 0,
      localbytes: RwLock::new(localbytes),
      source: path,
      server: server.to_string(),
      metadata: meta,
      new_blobs: RwLock::new(to_upload),
      touched_blobs: RwLock::new(HashMap::new()),
    })
  }

  fn local_path(&self, hash: &BlobHash) -> PathBuf {
    // As far as I can tell from online references there's no penalty in ext4 for
    // random lookup in a directory with lots of files. So just store all the hashed
    // files in a straight directory with no fanout to not waste space with directory
    // entries. Just doing a 12bit fanout (4096 directories) wastes 17MB on ext4.
    let mut path = self.source.clone();
    path.push(hex::encode(hash));
    path
  }

  fn remote_path(&self, hash: &BlobHash) -> String {
    let mut remote = self.server.clone();
    remote.push_str(&"/");
    remote.push_str(&hex::encode(hash));
    remote
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
    {
      let mut touched = self.touched_blobs.write().unwrap();
      touched.insert(hash.clone(), timeval());
    }
    let file = self.local_path(hash);
    if !file.exists() {
      try!(self.fetch_from_server(hash));
      let blob = try!(Blob::load(&file));
      let mut localbytes = self.localbytes.write().unwrap();
      *localbytes += blob.data.len() as u64;
      self.metadata.mark_deleted_blob(&hash, false);
      Ok(blob)
    } else {
      Blob::load(&file)
    }
  }

  fn store_blob(&self, blob: Blob) -> Result<(), c_int> {
    let file = self.local_path(&blob.hash);
    try!(blob.store(&file));
    try!(self.metadata.set_blob(&blob.hash, blob.data.len() as u64));
    let mut new_blobs = self.new_blobs.write().unwrap();
    new_blobs.push_back(blob.hash);
    let mut bytes = self.localbytes.write().unwrap();
    *bytes += blob.data.len() as u64;
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

  fn upload_to_server(&self, hash: &BlobHash) -> Result<(), c_int> {
    let path = self.local_path(hash);
    if !path.exists() {
      return Err(libc::EIO);
    }
    for _ in 0..10 {
      match Command::new("rsync").arg("--timeout=5").arg(&path).arg(&self.server).status() {
        Ok(_) => return Ok(()),
        Err(_) => {},
      }
    }
    eprintln!("Failed to upload block to server");
    Err(libc::EIO)
  }

  fn fetch_from_server(&self, hash: &BlobHash) -> Result<(), c_int> {
    let remote = self.remote_path(hash);
    for _ in 0..10 {
      match Command::new("rsync").arg("--timeout=5").arg(&remote).arg(&self.source).status() {
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
      match self.upload_to_server(&hash) {
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

  pub fn do_removals(&self) {
    {
      let mut touched = self.touched_blobs.write().unwrap();
      self.metadata.touch_blobs(touched.drain());
    }

    let bytes_to_delete = {
      let localbytes = self.localbytes.read().unwrap();
      if *localbytes > self.maxbytes { *localbytes - self.maxbytes } else { return; }
    };

    let mut deleted_bytes = 0;
    while deleted_bytes < bytes_to_delete {
      let hashes_to_delete = self.metadata.to_delete();
      if hashes_to_delete.len() == 0 {
        eprintln!("There's nothing else to delete even though I still need to reclaim space!");
        break;
      }
      for (hash, size) in hashes_to_delete {
        let path = self.local_path(&hash);
        match fs::remove_file(&path) {
          Ok(_) => {
            deleted_bytes += size;
            self.metadata.mark_deleted_blob(&hash, true);
          },
          Err(_) => eprintln!("Couldn't delete {:?}", path),
        };
      }
    }

    let mut localbytes = self.localbytes.write().unwrap();
    *localbytes -= deleted_bytes;
  }
}
