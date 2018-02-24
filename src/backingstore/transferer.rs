extern crate hex;
extern crate libc;
use super::blobstorage::*;

use std::path::PathBuf;
use std::process::Command;
use self::libc::c_int;

pub struct Transferer {
  source: PathBuf,
  server: String,
}

impl Transferer {
  pub fn new(source: PathBuf, server: String) -> Self {
    Self {
      source,
      server,
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
    let remote = self.remote_path(hash);
    for _ in 0..10 {
      let mut cmd = self.connect_to_server();
      cmd.arg(&remote);
      cmd.arg(&self.source);
      match cmd.status() {
        Ok(_) => return Ok(()),
        Err(_) => {},
      }
    }
    eprintln!("Failed to get block from server");
    Err(libc::EIO)
  }

  fn connect_to_server(&self) -> Command {
    let mut cmd = Command::new("rsync");
    cmd.arg("--quiet");
    cmd.arg("--timeout=5");
    cmd.arg("--whole-file");
    cmd
  }
}
