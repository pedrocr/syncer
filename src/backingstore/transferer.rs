extern crate hex;
extern crate libc;

use super::blobstorage::*;
use rwhashes::*;

use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};
use self::libc::c_int;

pub struct Transferer {
  source: PathBuf,
  server: String,
  ongoing: RwHashes<BlobHash, Arc<Mutex<bool>>>,
}

impl Transferer {
  pub fn new(source: PathBuf, server: String) -> Self {
    Self {
      source,
      server,
      ongoing: RwHashes::new(8),
    }
  }

  pub fn local_path(&self, hash: &BlobHash) -> PathBuf {
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

  pub fn upload_to_server(&self, hashes: &[BlobHash]) -> Result<(), c_int> {
    for _ in 0..10 {
      let mut cmd = self.connect_to_server();
      for hash in hashes {
        let path = self.local_path(hash);
        if !path.exists() {
          eprintln!("ERROR: couldn't find file {:?} to upload!", path);
        } else {
          cmd.arg(&path);
        }
      }
      cmd.arg(&self.server);
      match cmd.status() {
        Ok(_) => return Ok(()),
        Err(_) => {},
      }
    }
    eprintln!("ERROR: Failed to upload blocks to server");
    Err(libc::EIO)
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
    eprintln!("Fetching from server {}", hex::encode(hash));

    let remote = self.remote_path(hash);
    for _ in 0..10 {
      let mut cmd = self.connect_to_server();
      cmd.arg(&remote);
      cmd.arg(&self.source);
      match cmd.status() {
        Ok(_) => return true,
        Err(_) => {},
      }
    }
    eprintln!("Failed to get block from server");
    false
  }

  fn connect_to_server(&self) -> Command {
    let mut cmd = Command::new("rsync");
    cmd.arg("--quiet");
    cmd.arg("--timeout=5");
    cmd.arg("--whole-file");
    cmd
  }
}
