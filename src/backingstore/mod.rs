extern crate bincode;
extern crate libc;

mod blobstorage;
mod metadatadb;

use self::blobstorage::*;
pub use self::blobstorage::BlobHash;
use super::filesystem::FSEntry;

use self::bincode::{serialize, deserialize, Infinite};
use self::libc::c_int;
use std::sync::{Mutex, RwLock};
use std::collections::HashMap;

pub struct BackingStore {
  blobs: BlobStorage,
  node_counter: Mutex<u64>,
  node_cache: RwLock<HashMap<u64, FSEntry>>,
  zero: BlobHash,
}

impl BackingStore {
  pub fn new(path: &str, server: &str, maxbytes: u64) -> Result<Self, c_int> {
    let bs = try!(BlobStorage::new(path, server, maxbytes));
    let zero = BlobStorage::zero(1);
    let nodecount = try!(bs.max_node()) + 1;

    let out = Self {
      blobs: bs,
      node_counter: Mutex::new(nodecount),
      node_cache: RwLock::new(HashMap::new()),
      zero: zero,
    };
    try!(out.add_blob(&[0]));
    Ok(out)
  }

  pub fn blob_zero(&self) -> BlobHash {
    self.zero
  }

  pub fn add_blob(&self, data: &[u8]) -> Result<BlobHash, c_int> {
    self.blobs.add_blob(data)
  }

  pub fn create_node(&self, entry: FSEntry) -> Result<u64, c_int> {
    let node = {
      let mut counter = self.node_counter.lock().unwrap();
      *counter += 1;
      *counter
    };
    try!(self.save_node(node, entry));
    Ok(node)
  }

  pub fn save_node(&self, node: u64, entry: FSEntry) -> Result<(), c_int> {
    let encoded: Vec<u8> = serialize(&entry, Infinite).unwrap();
    try!(self.blobs.add_node(node, &encoded));
    Ok(())
  }

  pub fn save_node_cached(&self, node: u64, entry: FSEntry) -> Result<(), c_int> {
    let mut nodes = self.node_cache.write().unwrap();
    nodes.insert(node, entry);
    Ok(())
  }

  pub fn get_node(&self, node: u64) -> Result<FSEntry, c_int> {
    let nodes = self.node_cache.read().unwrap();
    match nodes.get(&node) {
      Some(n) => Ok((*n).clone()),
      None => {
        // We're in the slow path where we actually need to fetch stuff from disk
        let buffer = try!(self.blobs.read_node(node));
        Ok(deserialize(&buffer[..]).unwrap())
      },
    }
  }

  pub fn read(&self, node: u64, block: usize, hash: &BlobHash, offset: usize, bytes: usize, readahead: &[BlobHash]) -> Result<Vec<u8>, c_int> {
    self.blobs.read(node, block, hash, offset, bytes, readahead)
  }

  pub fn write(&self, node: u64, block: usize, hash: &BlobHash, offset: usize, data: &[u8], readahead: &[BlobHash]) -> Result<(), c_int> {
    self.blobs.write(node, block, hash, offset, data, readahead)
  }

  fn sync_one_node(&self, node: u64, mut entry: FSEntry) -> Result<(), c_int> {
    for (i, hash) in try!(self.blobs.sync_node(node)) {
      entry.set_block(i, hash);
    }
    try!(self.save_node(node, entry));
    Ok(())
  }

  pub fn sync_node(&self, node: u64) -> Result<(), c_int> {
    let mut nodes = self.node_cache.write().unwrap();
    if let Some(entry) = nodes.remove(&node) {
      try!(self.sync_one_node(node, entry));
    }
    self.blobs.do_save();
    Ok(())
  }

  pub fn sync_all(&self) -> Result<(), c_int> {
    let mut nodes = self.node_cache.write().unwrap();
    for (node, entry) in nodes.drain() {
      try!(self.sync_one_node(node, entry));
    }
    self.blobs.do_save();
    Ok(())
  }

  pub fn do_uploads(&self) {
    self.blobs.do_uploads();
  }

  pub fn do_removals(&self) {
    self.blobs.do_removals();
  }
}
