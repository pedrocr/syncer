extern crate bincode;
extern crate libc;
extern crate crossbeam_utils;

mod blobstorage;
mod metadatadb;
mod rsync;

use self::blobstorage::*;
pub use self::blobstorage::BlobHash;
use super::filesystem::FSEntry;
use crate::rwhashes::*;
use crate::config::*;

use self::libc::c_int;
use std::sync::Mutex;
use std::path::Path;
use std::io::{Error, ErrorKind};

pub type NodeId = (i64, i64);

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeInfo {
  pub id: NodeId,
  pub hash: BlobHash,
  pub creation: i64,
}

pub struct BackingStore {
  peernum: i64,
  blobs: BlobStorage,
  node_counter: Mutex<i64>,
  node_cache: RwHashes<NodeId, FSEntry>,
  zero: BlobHash,
}

impl BackingStore {
  pub fn new(path: &Path, config: &Config) -> Result<Self, c_int> {
    let bs = BlobStorage::new(&config.peerid, path, &config.server, config.maxbytes)?;
    let zero = BlobStorage::zero(1);
    let nodecount = bs.max_node(config.peernum())? + 1;

    let out = Self {
      blobs: bs,
      peernum: config.peernum(),
      node_counter: Mutex::new(nodecount),
      node_cache: RwHashes::new(8),
      zero: zero,
    };
    out.add_blob(&[0])?;
    Ok(out)
  }

  pub fn blob_zero(&self) -> BlobHash {
    self.zero
  }

  pub fn add_blob(&self, data: &[u8]) -> Result<BlobHash, c_int> {
    self.blobs.add_blob(data)
  }

  pub fn create_node(&self, entry: FSEntry) -> Result<NodeId, c_int> {
    let node = {
      let mut counter = self.node_counter.lock().unwrap();
      *counter += 1;
      (self.peernum, *counter)
    };
    self.save_node(node, entry)?;
    Ok(node)
  }

  pub fn save_node(&self, node: NodeId, entry: FSEntry) -> Result<(), c_int> {
    self.blobs.save_node(node, &entry)
  }

  pub fn save_node_cached(&self, node: NodeId, entry: FSEntry) -> Result<(), c_int> {
    let mut nodes = self.node_cache.write(&node);
    nodes.insert(node, entry);
    Ok(())
  }

  pub fn get_node(&self, node: NodeId) -> Result<FSEntry, c_int> {
    let nodes = self.node_cache.read(&node);
    match nodes.get(&node) {
      Some(n) => Ok((*n).clone()),
      None => {
        // We're in the slow path where we actually need to fetch stuff from disk
        let (_, entry) = self.fetch_node(node)?;
        Ok(entry)
      },
    }
  }

  pub fn fetch_node(&self, node: NodeId) -> Result<(BlobHash, FSEntry), c_int> {
    let (hash, buffer) = self.blobs.read_node(node)?;
    Ok((hash, bincode::deserialize(&buffer[..]).unwrap()))
  }

  pub fn node_exists(&self, node: NodeId) -> Result<bool, c_int> {
    let nodes = self.node_cache.read(&node);
    Ok(match nodes.get(&node) {
      Some(_) => true,
      None => self.blobs.node_exists(node)?,
    })
  }

  pub fn read(&self, node: NodeId, block: usize, hash: &BlobHash, offset: usize, bytes: usize, readahead: &[BlobHash]) -> Result<Vec<u8>, c_int> {
    self.blobs.read(node, block, hash, offset, bytes, readahead)
  }

  pub fn write(&self, node: NodeId, block: usize, hash: &BlobHash, offset: usize, data: &[u8], readahead: &[BlobHash]) -> Result<(), c_int> {
    self.blobs.write(node, block, hash, offset, data, readahead)
  }

  fn sync_one_node(&self, node: NodeId, mut entry: FSEntry) -> Result<(), c_int> {
    for (i, hash) in self.blobs.sync_node(node)? {
      entry.set_block(i, hash);
    }
    self.save_node(node, entry)?;
    Ok(())
  }

  pub fn sync_node(&self, node: NodeId) -> Result<(), c_int> {
    let mut nodes = self.node_cache.write(&node);
    if let Some(entry) = nodes.remove(&node) {
      self.sync_one_node(node, entry)?;
    }
    self.blobs.do_save();
    Ok(())
  }

  pub fn sync_all(&self) -> Result<(), Error> {
    for i in 0..self.node_cache.len() {
      let mut nodes = self.node_cache.write_pos(i);
      for (node, entry) in nodes.drain() {
        match self.sync_one_node(node, entry) {
          Ok(_) => {},
          Err(_) => return Err(Error::new(ErrorKind::Other, "sync failed")),
        }
      }
    }
    self.blobs.do_save();
    Ok(())
  }

  pub fn fsync_node(&self, node: NodeId) -> Result<(), c_int> {
    let (hash, entry) = self.fetch_node(node)?;
    self.blobs.fsync_file(&hash)?;
    for hash in entry.get_blocks() {
      self.blobs.fsync_file(&hash)?;
    }
    Ok(())
  }

  pub fn do_uploads(&self) -> Result<(), Error> {
    match self.blobs.do_uploads() {
      Ok(_) => Ok(()),
      Err(_) => Err(Error::new(ErrorKind::Other, "upload failed")),
    }
  }

  pub fn do_uploads_nodes(&self) -> Result<(), Error> {
    self.blobs.do_uploads_nodes()
  }

  pub fn do_downloads_nodes(&self) -> Result<(), Error> {
    self.blobs.do_downloads_nodes()
  }

  pub fn do_removals(&self) -> Result<(), Error> {
    self.blobs.do_removals()
  }

  pub fn init_server(&self) -> Result<(), Error> {
    self.blobs.init_server()?;
    self.sync_all()?;
    self.do_uploads()?;
    self.do_uploads_nodes()?;
    Ok(())
  }
}
