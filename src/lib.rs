#[macro_use] extern crate serde_derive;
extern crate fuse_mt;
use self::fuse_mt::*;
extern crate crossbeam_utils;
use self::crossbeam_utils::thread::Scope;
use self::crossbeam_utils::thread::ScopedJoinHandle;
extern crate base64;
extern crate bincode;
extern crate hex;

use std::io::{Error, ErrorKind};
use std::ffi::{OsStr};
use std::time;
use std::mem;
use std::sync::mpsc;
use std::path::{Path, PathBuf};
use std::io::{Read, BufRead, BufReader};
use std::fs::File;

mod filesystem;
mod backingstore;
mod settings;
mod rwhashes;
pub mod config;

use crate::settings::*;
use crate::config::*;

use self::backingstore::BackingStore;
use self::filesystem::FS;

// This is a hack while FuseMT requires 'static for the FilesystemMT instance
// See the github issue for discussion: https://github.com/wfraser/fuse-mt/issues/26
fn fix_lifetime<'a>(t: FS<'a>) -> FS<'static> {
  unsafe { mem::transmute(t) }
}

struct BackgroundThread<'a> {
  handle: ScopedJoinHandle<'a, ()>,
  tx: std::sync::mpsc::Sender<u8>,
}

impl<'a> BackgroundThread<'a> {
  fn new<F: 'a>(scope: &Scope<'a>, secs: u64, closure: F) -> Self
  where F: Fn() -> Result<(), Error> + Send {
    let (tx, rx) = mpsc::channel();

    let handle = scope.spawn(move || {
      // Sync the backing store to disk/remote every so often
      let dur = time::Duration::from_secs(secs);
      loop {
        match rx.recv_timeout(dur) {
          Err(mpsc::RecvTimeoutError::Timeout) => {
            match closure() {
              Ok(_) => {},
              Err(e) => eprintln!("ERROR: {}", e),
            }
          },
          _ => break,
        }
      }
    });

    Self {
      handle,
      tx,
    }
  }

  fn join(self) {
    self.tx.send(0).unwrap(); // Signal the thread to die
    self.handle.join().unwrap();
  }
}

pub fn run(source: &Path, mount: &Path, conf: &Config) -> Result<(), Error> {
  if conf.formatversion < FORMATVERSION {
    let message = format!("Trying to mount old format (version {} vs {})",
                           conf.formatversion, FORMATVERSION);
    return Err(Error::new(ErrorKind::Other, message));
  }

  let bs = match BackingStore::new(source, &conf) {
    Ok(bs) => bs,
    Err(_) => return Err(Error::new(ErrorKind::Other, "Couldn't create the backing store")),
  };
  let fs = match filesystem::FS::new(&bs, conf.peernum()) {
    Ok(fs) => fs,
    Err(_) => return Err(Error::new(ErrorKind::Other, "Couldn't create the filesystem")),
  };
  let fs = fix_lifetime(fs);
  let bsref = &bs;

  crossbeam_utils::thread::scope(|scope| {
    let sync   = BackgroundThread::new(&scope, 60, move || bsref.sync_all());
    let upload = BackgroundThread::new(&scope, 10, move || bsref.do_uploads());
    let nodes1 = BackgroundThread::new(&scope, 10, move || bsref.do_uploads_nodes());
    let nodes2 = BackgroundThread::new(&scope, 10, move || bsref.do_downloads_nodes());
    let remove = BackgroundThread::new(&scope, 10, move || bsref.do_removals());

    let fshandle = scope.spawn(move || {
      let fs_mt = FuseMT::new(fs, 16);
      let options = [OsStr::new("-o"), OsStr::new("auto_unmount,default_permissions")];
      fuse_mt::mount(fs_mt, &mount, &options[..])
    });

    let ret = fshandle.join();
    sync.join();
    upload.join();
    nodes1.join();
    nodes2.join();
    remove.join();
    ret
  }).unwrap()
}

pub fn clone(source: &Path, conf: &Config) -> Result<(), Error> {
  if conf.formatversion < FORMATVERSION {
    let message = format!("Trying to clone into old format (version {} vs {})",
                           conf.formatversion, FORMATVERSION);
    return Err(Error::new(ErrorKind::Other, message));
  }

  let bs = match BackingStore::new(source, &conf) {
    Ok(bs) => bs,
    Err(_) => return Err(Error::new(ErrorKind::Other, "Couldn't create the backing store")),
  };

  bs.do_downloads_nodes()?;

  Ok(())
}

pub fn init(source: &Path, conf: &Config) -> Result<(), Error> {
  if conf.formatversion < FORMATVERSION {
    let message = format!("Trying to clone into old format (version {} vs {})",
                           conf.formatversion, FORMATVERSION);
    return Err(Error::new(ErrorKind::Other, message));
  }

  let bs = match BackingStore::new(source, &conf) {
    Ok(bs) => bs,
    Err(_) => return Err(Error::new(ErrorKind::Other, "Couldn't create the backing store")),
  };
  match filesystem::FS::new(&bs, conf.peernum()) {
    Ok(fs) => fs,
    Err(_) => return Err(Error::new(ErrorKind::Other, "Couldn't create the filesystem")),
  };

  bs.init_server()?;
  Ok(())
}

pub fn printlog(source: &Path, conf: &Config) -> Result<(), Error> {
  let mut log = PathBuf::from(source);
  log.push("nodes");
  log.push(&conf.peerid);

  let buffer = BufReader::new(File::open(&log).unwrap());
  for line in buffer.lines() {
    let line = line.unwrap();
    let buffer = base64::decode(&line).unwrap();
    let node: backingstore::NodeInfo = bincode::deserialize(&buffer).unwrap();
    let hash = hex::encode(&node.hash);
    println!("node {} -> {}, {:?}", hash, node.creation, node.id);
    let mut blobpath = PathBuf::from(source);
    blobpath.push("blobs");
    blobpath.push(hash);
    let mut buffer = Vec::new();
    File::open(&blobpath).unwrap().read_to_end(&mut buffer).unwrap();
    let entry: filesystem::FSEntry = bincode::deserialize(&buffer).unwrap();
    println!("entry {:?}", entry);
  }

  Ok(())
}
