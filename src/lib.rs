#[macro_use] extern crate serde_derive;
extern crate fuse_mt;
use self::fuse_mt::*;
extern crate crossbeam;

use std::io::{Error, ErrorKind};
use std::ffi::{OsStr};
use std::time;
use std::mem;
use std::sync::mpsc;
use std::path::Path;

mod filesystem;
mod backingstore;
mod settings;
mod rwhashes;
pub mod config;

use settings::*;
use config::*;

use self::backingstore::BackingStore;
use self::filesystem::FS;

// This is a hack while FuseMT requires 'static for the FilesystemMT instance
// See the github issue for discussion: https://github.com/wfraser/fuse-mt/issues/26
fn fix_lifetime<'a>(t: FS<'a>) -> FS<'static> {
  unsafe { mem::transmute(t) }
}

struct BackgroundThread {
  handle: crossbeam::ScopedJoinHandle<()>,
  tx: std::sync::mpsc::Sender<u8>,
}

impl BackgroundThread {
  fn new<'a, F: 'a>(scope: &crossbeam::Scope<'a>, secs: u64, closure: F) -> Self
  where F: Fn()+Send {
    let (tx, rx) = mpsc::channel();

    let handle = scope.spawn(move || {
      // Sync the backing store to disk/remote every so often
      let dur = time::Duration::from_secs(secs);
      loop {
        match rx.recv_timeout(dur) {
          Err(mpsc::RecvTimeoutError::Timeout) => {
            closure();
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
    self.handle.join();
  }
}

pub fn run(source: &Path, mount: &Path, conf: &Config) -> Result<(), Error> {
  if conf.formatversion < FORMATVERSION {
    let message = format!("Trying to mount old format (version {} vs {})",
                           conf.formatversion, FORMATVERSION);
    return Err(Error::new(ErrorKind::Other, message));
  }

  let bs = match BackingStore::new(&conf.peerid, conf.peernum(), source, &conf.server, conf.maxbytes) {
    Ok(bs) => bs,
    Err(_) => return Err(Error::new(ErrorKind::Other, "Couldn't create the backing store")),
  };
  let fs = match filesystem::FS::new(&bs) {
    Ok(fs) => fs,
    Err(_) => return Err(Error::new(ErrorKind::Other, "Couldn't create the filesystem")),
  };
  let fs = fix_lifetime(fs);
  let bsref = &bs;

  crossbeam::scope(|scope| {
    let sync   = BackgroundThread::new(&scope, 60, move || bsref.sync_all().unwrap());
    let upload = BackgroundThread::new(&scope, 10, move || bsref.do_uploads());
    let nodes  = BackgroundThread::new(&scope, 10, move || bsref.do_uploads_nodes());
    let remove = BackgroundThread::new(&scope, 10, move || bsref.do_removals());

    let fshandle = scope.spawn(move || {
      let fs_mt = FuseMT::new(fs, 16);
      let options = [OsStr::new("-o"), OsStr::new("auto_unmount,default_permissions")];
      fuse_mt::mount(fs_mt, &mount, &options[..])
    });

    let ret = fshandle.join();
    sync.join();
    upload.join();
    nodes.join();
    remove.join();
    ret
  })
}
