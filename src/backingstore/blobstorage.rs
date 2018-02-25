extern crate rusqlite;
extern crate blake2;
extern crate hex;
extern crate libc;
extern crate bincode;

use super::metadatadb::*;
use super::transferer::*;
use settings::*;
use rwhashes::*;
use self::bincode::serialize;
use self::rusqlite::Connection;
use self::blake2::Blake2b;
use self::blake2::digest::{Input, VariableOutput};
use self::libc::c_int;
use std::cmp;
use std::path::{Path, PathBuf};
use std::fs;
use std::io::prelude::*;
use std::usize;
use std::collections::HashMap;
use std::sync::RwLock;
use std::fs::OpenOptions;

pub type BlobHash = [u8;HASHSIZE];

#[derive(Clone)]
pub struct Blob {
  data: Vec<u8>,
}

impl Blob {
  pub fn zero(size: usize) -> Self {
    Self::new_with_data(vec![0 as u8; size])
  }

  pub fn new_with_data(data: Vec<u8>) -> Self {
    Self {
      data,
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

  fn write(&mut self, offset: usize, data: &[u8]) {
    let start = offset;
    let end = offset+data.len();
    if end > self.data.len() { self.data.resize(end, 0) }
    self.data[start..end].copy_from_slice(&data[..]);
  }

  fn hash(&self) -> BlobHash {
    let mut hasher = Blake2b::new(HASHSIZE).unwrap();
    hasher.process(&self.data);
    let mut buf = [0u8; HASHSIZE];
    hasher.variable_result(&mut buf).unwrap();
    buf
  }
}

pub struct BlobStorage {
  maxbytes: u64,
  source: PathBuf,
  transferer: Transferer,
  metadata: MetadataDB,
  written_blobs: RwLock<Vec<(BlobHash, u64, i64)>>,
  touched_blobs: RwLock<HashMap<BlobHash,i64>>,
  blob_cache: RwHashes<u64, HashMap<usize, Blob>>,
}

impl BlobStorage {
  pub fn new(source: &Path, server: &str, maxbytes: u64) -> Result<Self, c_int> {
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

    Ok(BlobStorage {
      maxbytes,
      source: PathBuf::from(source),
      transferer: Transferer::new(path, server.to_string()),
      metadata: meta,
      written_blobs: RwLock::new(Vec::new()),
      touched_blobs: RwLock::new(HashMap::new()),
      blob_cache: RwHashes::new(8),
    })
  }

  pub fn fsync_file(&self, hash: &BlobHash) -> Result<(), c_int> {
    let path = self.transferer.local_path(hash);
    let file = match fs::File::open(&path) {
      Ok(f) => f,
      Err(_) => return Err(libc::EIO),
    };
    match file.sync_all() {
      Ok(_) => {},
      Err(_) => return Err(libc::EIO),
    }
    Ok(())
  }

  pub fn read(&self, node: u64, block: usize, hash: &BlobHash, offset: usize, bytes: usize, readahead: &[BlobHash]) -> Result<Vec<u8>, c_int> {
    // First figure out if this isn't a cached blob
    let blob_cache = self.blob_cache.read(&node);
    if let Some(blocks) = blob_cache.get(&node) {
      if let Some(blob) = blocks.get(&block) {
        return Ok(blob.read(offset, bytes))
      }
    }

    let blob = try!(self.get_blob(hash, readahead));
    Ok(blob.read(offset, bytes))
  }

  pub fn write(&self, node: u64, block: usize, hash: &BlobHash, offset: usize, data: &[u8], readahead: &[BlobHash]) -> Result<(), c_int> {
    // First figure out if this isn't a cached blob
    {
      let mut blob_cache = self.blob_cache.write(&node);
      if let Some(blocks) = blob_cache.get_mut(&node) {
        if let Some(mut blob) = blocks.get_mut(&block) {
          return Ok(blob.write(offset, data))
        }
      }
    }

    let mut blob = try!(self.get_blob(hash, readahead));
    let hash = blob.write(offset, data);

    // Store the blob in the cache
    let mut blob_cache = self.blob_cache.write(&node);
    let blocks = blob_cache.entry(node).or_insert(HashMap::new());
    blocks.insert(block, blob);

    Ok(hash)
  }

  pub fn sync_node(&self, node: u64) -> Result<Vec<(usize, BlobHash)>, c_int> {
    let mut stored = Vec::new();
    let mut blob_cache = self.blob_cache.write(&node);
    if let Some(mut blocks) = blob_cache.remove(&node) {
      for (i, blob) in blocks.drain() {
        let hash = try!(self.store_blob(blob));
        stored.push((i, hash));
      }
    }
    Ok(stored)
  }

  fn get_blob(&self, hash: &BlobHash, readahead: &[BlobHash]) -> Result<Blob, c_int> {
    {
      let timeval = timeval();
      let mut touched = self.touched_blobs.write().unwrap();
      touched.insert(hash.clone(), timeval);
      for hash in readahead {
        if hash != &HASHZERO {
          touched.insert(hash.clone(), timeval);
        }
      }
    }
    let file = self.transferer.local_path(hash);
    if !file.exists() {
      self.transferer.readahead_from_server(readahead);
      try!(self.transferer.fetch_from_server(hash));
      let blob = try!(Blob::load(&file));
      Ok(blob)
    } else {
      Blob::load(&file)
    }
  }

  fn store_blob(&self, blob: Blob) -> Result<BlobHash, c_int> {
    let hash = blob.hash();
    let file = self.transferer.local_path(&hash);
    try!(blob.store(&file));
    {
      let mut written_blobs = self.written_blobs.write().unwrap();
      written_blobs.push((hash, blob.data.len() as u64, timeval()));
    }
    Ok(hash)
  }

  pub fn zero(size: usize) -> BlobHash {
    Blob::zero(size).hash()
  }

  pub fn add_blob(&self, data: &[u8]) -> Result<BlobHash, c_int> {
    let blob = Blob::new_with_data(data.to_vec());
    let hash = blob.hash();
    try!(self.store_blob(blob));
    Ok(hash)
  }

  pub fn max_node(&self) -> Result<u64, c_int> {
    self.metadata.max_node()
  }

  pub fn add_node(&self, node: u64, data: &[u8]) -> Result<BlobHash, c_int> {
    let hash = try!(self.add_blob(data));
    try!(self.metadata.set_node(node, &hash));
    Ok(hash)
  }

  pub fn read_node(&self, node: u64) -> Result<(BlobHash, Vec<u8>), c_int> {
    let hash = try!(self.metadata.get_node(node));
    let blob = try!(self.get_blob(&hash, &[]));
    Ok((hash, blob.read(0, usize::MAX)))
  }

  pub fn node_exists(&self, node: u64) -> Result<bool, c_int> {
    self.metadata.node_exists(node)
  }

  pub fn do_save(&self) {
    let mut written_blobs = self.written_blobs.write().unwrap();
    self.metadata.set_blobs(written_blobs.drain(..));
  }

  pub fn do_uploads(&self) {
    loop {
      let mut hashes = self.metadata.to_upload();
      if hashes.len() == 0 { break }
      if self.transferer.upload_to_server(&hashes).is_ok() {
        self.metadata.mark_synced_blobs(hashes.drain(..));
      }
    }
  }

  pub fn do_uploads_nodes(&self) {
    let mut path = self.source.clone();
    path.push("node_entries");
    let mut written = false;

    loop {
      let nodes = self.metadata.to_upload_nodes();
      if nodes.len() == 0 { break }
      let mut file = match OpenOptions::new().append(true).create(true).open(&path) {
        Err(e) => {eprintln!("ERROR: couldn't write to entries file: {}", e); break;},
        Ok(f) => f,
      };
      let mut synced = Vec::new();
      for nodeinfo in nodes {
        let mut encoded = hex::encode(serialize(&nodeinfo).unwrap());
        encoded.push('\n');
        match file.write_all(&encoded.into_bytes()) {
          Err(e) => {eprintln!("ERROR: couldn't write entry in entries file: {}", e); break;},
          Ok(_) => {synced.push(nodeinfo.rowid)},
        }
      }
      match file.sync_all() {
        Err(e) => {eprintln!("ERROR: couldn't fsync entries file: {}", e);},
        Ok(_) => {},
      }
      self.metadata.mark_synced_nodes(&synced);
      written = true;
    }

    if written {
      self.transferer.send(&path);
    }
  }

  pub fn do_removals(&self) {
    {
      let mut touched = self.touched_blobs.write().unwrap();
      self.metadata.touch_blobs(touched.drain());
    }

    let bytes_to_delete = {
      let localbytes = self.metadata.localbytes();
      if localbytes > self.maxbytes { localbytes - self.maxbytes } else { return; }
    };

    let mut deleted_bytes = 0;
    loop {
      let hashes_to_delete = self.metadata.to_delete();
      if hashes_to_delete.len() == 0 {
        eprintln!("WARNING: Nothing to delete but reclaim needed ({} bytes)", bytes_to_delete - deleted_bytes);
        break;
      }
      let mut deleted = Vec::new();
      for (hash, size) in hashes_to_delete {
        let path = self.transferer.local_path(&hash);
        let delete_worked = fs::remove_file(&path).is_ok();
        if !delete_worked {
          if !path.exists() {
            eprintln!("WARNING: tried to delete file that's already gone {:?}", path);
          } else {
            eprintln!("WARNING: failed to delete {:?}", path);
            continue; // We couldn't delete the file so space is not reclaimed
          }
        }

        deleted_bytes += size;
        deleted.push(hash);
        if deleted_bytes >= bytes_to_delete {
          break
        }
      }
      self.metadata.mark_deleted_blobs(&deleted, true);

      if deleted_bytes >= bytes_to_delete {
        break
      }
    }
  }
}
