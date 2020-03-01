extern crate rusqlite;
extern crate blake2;
extern crate hex;
extern crate base64;
extern crate libc;
extern crate bincode;
extern crate crossbeam_utils;

use super::metadatadb::*;
use super::rsync::*;
use super::{NodeInfo, NodeId};
use crate::settings::*;
use crate::rwhashes::*;
use crate::config::*;
use crate::filesystem::*;
use self::rusqlite::Connection;
use self::blake2::Blake2b;
use self::blake2::digest::{Input, VariableOutput};
use self::libc::c_int;
use std::cmp;
use std::path::{Path, PathBuf};
use std::fs;
use std::io::prelude::*;
use std::io::Error;
use std::{usize, i64};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, SeekFrom};
use std::fs::File;

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

  fn len(&self) -> usize {
    self.data.len()
  }
}

pub struct BlobStorage {
  maxbytes: u64,
  peerid: String,
  local: PathBuf,
  server: String,
  ongoing: RwHashes<BlobHash, Arc<Mutex<bool>>>,
  metadata: MetadataDB,
  written_blobs: RwLock<Vec<(BlobHash, u64, i64)>>,
  touched_blobs: RwLock<HashMap<BlobHash,(i64, usize)>>,
  blob_cache: RwHashes<NodeId, HashMap<usize, Blob>>,
}

impl BlobStorage {
  pub fn new(peerid: &str, source: &Path, server: &str, maxbytes: u64) -> Result<Self, c_int> {
    // Make sure the local blobs dir exists
    let mut path = PathBuf::from(source);
    path.push("blobs");
    match fs::create_dir_all(&path) {
      Ok(_) => {},
      Err(_) => return Err(libc::EIO),
    }

    // Make sure the local nodes dir exists
    let mut path = PathBuf::from(source);
    path.push("nodes");
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
      peerid: peerid.to_string(),
      local: PathBuf::from(source),
      server: server.to_string(),
      ongoing: RwHashes::new(8),
      metadata: meta,
      written_blobs: RwLock::new(Vec::new()),
      touched_blobs: RwLock::new(HashMap::new()),
      blob_cache: RwHashes::new(8),
    })
  }

  pub fn fsync_file(&self, hash: &BlobHash) -> Result<(), c_int> {
    let path = self.local_path(hash);
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

  pub fn read(&self, node: NodeId, block: usize, hash: &BlobHash, offset: usize, bytes: usize, readahead: &[BlobHash]) -> Result<Vec<u8>, c_int> {
    // First figure out if this isn't a cached blob
    let blob_cache = self.blob_cache.read(&node);
    if let Some(blocks) = blob_cache.get(&node) {
      if let Some(blob) = blocks.get(&block) {
        return Ok(blob.read(offset, bytes))
      }
    }

    let blob = self.get_blob(hash, readahead)?;
    Ok(blob.read(offset, bytes))
  }

  pub fn write(&self, node: NodeId, block: usize, hash: &BlobHash, offset: usize, data: &[u8], readahead: &[BlobHash]) -> Result<(), c_int> {
    // First figure out if this isn't a cached blob
    {
      let mut blob_cache = self.blob_cache.write(&node);
      if let Some(blocks) = blob_cache.get_mut(&node) {
        if let Some(blob) = blocks.get_mut(&block) {
          return Ok(blob.write(offset, data))
        }
      }
    }

    let mut blob = self.get_blob(hash, readahead)?;
    let hash = blob.write(offset, data);

    // Store the blob in the cache
    let mut blob_cache = self.blob_cache.write(&node);
    let blocks = blob_cache.entry(node).or_insert(HashMap::new());
    blocks.insert(block, blob);

    Ok(hash)
  }

  pub fn sync_node(&self, node: NodeId) -> Result<Vec<(usize, BlobHash)>, c_int> {
    let mut stored = Vec::new();
    let mut blob_cache = self.blob_cache.write(&node);
    if let Some(mut blocks) = blob_cache.remove(&node) {
      for (i, blob) in blocks.drain() {
        let hash = self.store_blob(blob)?;
        stored.push((i, hash));
      }
    }
    Ok(stored)
  }

  fn get_blob(&self, hash: &BlobHash, readahead: &[BlobHash]) -> Result<Blob, c_int> {
    self.readahead_from_server(readahead);
    let file = self.local_path(hash);
    if !file.exists() {
      self.fetch_from_server(hash)?;
    }
    let blob = Blob::load(&file)?;
    {
      let timeval = timeval();
      let mut touched = self.touched_blobs.write().unwrap();
      touched.insert(hash.clone(), (timeval, blob.len()));
    }
    Ok(blob)
  }

  fn store_blob(&self, blob: Blob) -> Result<BlobHash, c_int> {
    let hash = blob.hash();
    let file = self.local_path(&hash);
    blob.store(&file)?;
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
    self.store_blob(blob)?;
    Ok(hash)
  }

  pub fn max_node(&self, peernum: i64) -> Result<i64, c_int> {
    self.metadata.max_node(peernum)
  }

  pub fn save_node(&self, node: NodeId, entry: &FSEntry) -> Result<(), c_int> {
    let encoded: Vec<u8> = bincode::serialize(&entry).unwrap();
    let hash = self.add_blob(&encoded)?;
    if self.metadata.node_exists_long(node, &hash, entry.timeval())? {
      // this is a duplicate, skip it
      return Ok(())
    }
    if !self.metadata.node_exists(node)? {
      // this is the first of its kind push it
      self.metadata.set_node(node, &hash, entry.timeval())?;
      return Ok(())
    }
    let (hash2, buffer) = self.read_node(node)?;
    let currnode: FSEntry = bincode::deserialize(&buffer[..]).unwrap();
    match entry.cmp_vclock(&currnode) {
      VectorOrdering::Greater => {
        self.metadata.set_node(node, &hash, entry.timeval())?;
      },
      VectorOrdering::Less => {
        // Our current node is a later one so add the new one but behind it
        self.metadata.set_node_behind(node, &hash, entry.timeval())?;
      },
      VectorOrdering::Equal => {
        eprintln!("WARNING: found node {:?} with same vector clock that isn't identical", node);
        eprintln!("1st hash is {}", hex::encode(&hash));
        eprintln!("2nd hash is {}", hex::encode(&hash2));
        eprintln!("1st from peer {} is {:?}", entry.peernum, entry);
        eprintln!("2nd from peer {} is {:?}", currnode.peernum, currnode);
        self.metadata.set_node(node, &hash, entry.timeval())?;
      },
      VectorOrdering::Conflict => {
        // We're in a conflict situation, we're going to need to merge and for that we
        // need a common base to do the three way merge

        let base = self.read_earlier_node(node, entry)?;
        let merged = base.merge_3way(entry, &currnode);
        let encoded: Vec<u8> = bincode::serialize(&merged).unwrap();
        let hash = self.add_blob(&encoded)?;
        self.metadata.set_node(node, &hash, merged.timeval())?;
      },
    }
    Ok(())
  }

  pub fn read_node(&self, node: NodeId) -> Result<(BlobHash, Vec<u8>), c_int> {
    let hash = self.metadata.get_node(node)?;
    let blob = self.get_blob(&hash, &[])?;
    Ok((hash, blob.read(0, usize::MAX)))
  }

  pub fn read_earlier_node(&self, node: NodeId, comparison: &FSEntry) -> Result<FSEntry, c_int> {
    let mut maxrowid = i64::MAX;
    loop {
      let (row, hash) = self.metadata.get_earlier_node(node, maxrowid)?;
      maxrowid = row;
      let blob = self.get_blob(&hash, &[])?;
      let encoded = blob.read(0, usize::MAX);
      let entry: FSEntry = bincode::deserialize(&encoded[..]).unwrap();
      if comparison.cmp_vclock(&entry) == VectorOrdering::Greater {
        return Ok(entry)
      }
    }
  }

  pub fn node_exists(&self, node: NodeId) -> Result<bool, c_int> {
    self.metadata.node_exists(node)
  }

  pub fn do_save(&self) {
    let mut written_blobs = self.written_blobs.write().unwrap();
    self.metadata.set_blobs(written_blobs.drain(..));
  }

  pub fn do_uploads(&self) -> Result<(), c_int> {
    loop {
      let mut hashes = self.metadata.to_upload();
      if hashes.len() == 0 { break }
      self.upload_to_server(&hashes)?;
      self.metadata.mark_synced_blobs(hashes.drain(..));
    }
    Ok(())
  }

  pub fn init_server(&self) -> Result<(), Error> {
    let mut cmd = RsyncCommand::new();
    cmd.arg("-r");
    cmd.arg("--exclude=metadata*");
    cmd.arg(&self.local);
    cmd.arg(&self.server);
    cmd.run()
  }

  pub fn do_uploads_nodes(&self) -> Result<(), Error> {
    let mut path = self.local.clone();
    path.push("nodes");
    path.push(&self.peerid.to_string());
    let mut written = false;

    loop {
      let nodes = self.metadata.to_upload_nodes();
      if nodes.len() == 0 { break }
      let mut file = match OpenOptions::new().append(true).create(true).open(&path) {
        Err(e) => {eprintln!("ERROR: couldn't write to entries file: {}", e); break;},
        Ok(f) => f,
      };
      let mut synced = Vec::new();
      for (rowid, nodeinfo) in nodes {
        let mut encoded = base64::encode(&bincode::serialize(&nodeinfo).unwrap());
        encoded.push('\n');
        match file.write_all(&encoded.into_bytes()) {
          Err(e) => {eprintln!("ERROR: couldn't write entry in entries file: {}", e); break;},
          Ok(_) => {synced.push(rowid)},
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
      let mut remote = self.server.clone();
      remote.push_str(&"/data/nodes/");
      let mut cmd = RsyncCommand::new();
      cmd.arg(&path);
      cmd.arg(&remote);
      return cmd.run();
    }

    Ok(())
  }

  pub fn do_downloads_nodes(&self) -> Result<(), Error> {
    let mut path = self.local.clone();
    path.push("nodes");
    let mut remote = self.server.clone();
    remote.push_str(&"/data/nodes/");

    // First fetch all the nodes files in the server except our own
    let mut cmd = RsyncCommand::new();
    cmd.arg("-r");
    cmd.arg(format!("--exclude={}", self.peerid));
    cmd.arg(&remote);
    cmd.arg(&path);
    cmd.run()?;

    for file in fs::read_dir(&path).unwrap() {
      let path = file.unwrap().path();
      if path.is_dir() { continue }
      let filename: String = path.file_name().unwrap().to_str().unwrap().to_string();
      if filename.len() != 16 { continue }
      if filename == self.peerid { continue }
      let peernum = convert_peerid(&filename);

      let mut buffer = BufReader::new(File::open(&path).unwrap());
      let mut offset = self.metadata.get_peer(peernum).unwrap();
      buffer.seek(SeekFrom::Start(offset)).unwrap();

      for line in buffer.lines() {
        let line = line.unwrap();
        offset += line.len() as u64 + 1;
        let buffer = base64::decode(&line).unwrap();
        let node: NodeInfo = bincode::deserialize(&buffer).unwrap();
        let blob = self.get_blob(&node.hash, &[]).unwrap();
        let entry: FSEntry = bincode::deserialize(&blob.read(0, usize::MAX)).unwrap();
        self.save_node(node.id, &entry).unwrap();
        self.metadata.set_peer(peernum, offset).unwrap();
      }
    }

    Ok(())
  }

  pub fn do_removals(&self) -> Result<(), Error> {
    {
      let mut touched = self.touched_blobs.write().unwrap();
      self.metadata.touch_blobs(touched.drain());
    }

    let bytes_to_delete = {
      let localbytes = self.metadata.localbytes();
      if localbytes > self.maxbytes { localbytes - self.maxbytes } else { return Ok(()) }
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
        let path = self.local_path(&hash);
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

    Ok(())
  }

  pub fn local_path(&self, hash: &BlobHash) -> PathBuf {
    // As far as I can tell from online references there's no penalty in ext4 for
    // random lookup in a directory with lots of files. So just store all the hashed
    // files in a straight directory with no fanout to not waste space with directory
    // entries. Just doing a 12bit fanout (4096 directories) wastes 17MB on ext4.
    let mut path = self.local.clone();
    path.push("blobs");
    path.push(hex::encode(hash));
    path
  }

  fn remote_path(&self, hash: &BlobHash) -> String {
    let mut remote = self.server.clone();
    remote.push_str(&"/data/blobs/");
    remote.push_str(&hex::encode(hash));
    remote
  }

  pub fn upload_to_server(&self, hashes: &[BlobHash]) -> Result<(), c_int> {
    let mut cmd = RsyncCommand::new();
    for hash in hashes {
      let path = self.local_path(hash);
      if !path.exists() {
        eprintln!("ERROR: couldn't find file {:?} to upload!", path);
      } else {
        cmd.arg(&path);
      }
    }
    let mut remote = self.server.clone();
    remote.push_str(&"/data/blobs/");
    cmd.arg(&remote);
    match cmd.run() {
      Ok(_) => return Ok(()),
      Err(_) => {},
    }
    eprintln!("ERROR: Failed to upload blocks to server");
    Err(libc::EIO)
  }

  pub fn readahead_from_server<'a>(&'a self, hashes: &[BlobHash]) {
    for hash in hashes {
      if hash != &HASHZERO && !self.local_path(hash).exists() {
        let hash = hash.clone();
        unsafe{crossbeam_utils::thread::spawn_unchecked(move || {
          let mut ongoing = self.ongoing.write(&hash);
          if !ongoing.contains_key(&hash) {
            let mutex = Arc::new(Mutex::new(false));
            let mut res = mutex.lock().unwrap();
            ongoing.insert(hash.clone(), mutex.clone());
            drop(ongoing);
            *res = self.real_fetch_from_server(&hash);
            {
              let mut ongoing = self.ongoing.write(&hash); // Grab the lock again
              ongoing.remove(&hash); // Remove from the hash as it's already done now
            }
            // If we've loaded the file we need to make sure it gets touch()ed so that
            // it shows up in the blobs table if it didn't exist before
            let file = self.local_path(&hash);
            match Blob::load(&file) {
              Err(_) => {}, 
              Ok(blob) => {
                let timeval = timeval();
                let mut touched = self.touched_blobs.write().unwrap();
                touched.insert(hash.clone(), (timeval, blob.len()));
              },
            }
          }
        });}
      }
    }
  }

  pub fn fetch_from_server(&self, hash: &BlobHash) -> Result<(), c_int> {
    let mutex = {
      let mut ongoing = self.ongoing.write(hash);
      if ongoing.contains_key(hash) {
        // Another thread has started this fetch, just return the lock for it to wait on it
        ongoing.get(hash).unwrap().clone()
      } else {
        // We're doing it live ourselves
        let mutex = Arc::new(Mutex::new(false));
        let mut res = mutex.lock().unwrap();
        ongoing.insert(hash.clone(), mutex.clone());
        drop(ongoing); // Don't hold the lock so other threads can now fetch stuff
        *res = self.real_fetch_from_server(hash);
        let mut ongoing = self.ongoing.write(hash); // Grab the lock again
        ongoing.remove(hash); // Remove from the hash as it's already done now
        return if *res {Ok(())} else {Err(libc::EIO)}
      }
    };

    let res = mutex.lock().unwrap();
    if *res {Ok(())} else {Err(libc::EIO)}
  }

  fn real_fetch_from_server(&self, hash: &BlobHash) -> bool {
    let remote = self.remote_path(hash);
    let mut cmd = RsyncCommand::new();
    cmd.arg(&remote);
    let mut path = self.local.clone();
    path.push("blobs");
    cmd.arg(&path);
    match cmd.run() {
      Ok(_) => true,
      Err(_) => false,
    }
  }
}
