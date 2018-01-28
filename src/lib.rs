#[macro_use] extern crate lazy_static;
#[macro_use] extern crate serde_derive;
extern crate fuse_mt;
use self::fuse_mt::*;
extern crate crossbeam;

use std::io::{Error, ErrorKind};
use std::ffi::{OsStr};
use std::time;
use std::mem;
use std::sync::mpsc;

mod filesystem;
mod backingstore;

use self::backingstore::BackingStore;
use self::filesystem::FS;

// This is a hack while FuseMT requires 'static for the FilesystemMT instance
// See the github issue for discussion: https://github.com/wfraser/fuse-mt/issues/26
fn fix_lifetime<'a>(t: FS<'a>) -> FS<'static> {
  unsafe { mem::transmute(t) }
}

pub fn run(source: &str, mount: &str) -> Result<(), Error> {
  let bs = match BackingStore::new(source) {
    Ok(bs) => bs,
    Err(_) => return Err(Error::new(ErrorKind::Other, "Couldn't create the backing store")),
  };
  let fs = match filesystem::FS::new(&bs) {
    Ok(fs) => fs,
    Err(_) => return Err(Error::new(ErrorKind::Other, "Couldn't create the filesystem")),
  };
  let fs = fix_lifetime(fs);
  let bsref = &bs;
  let (tx, rx) = mpsc::channel();

  crossbeam::scope(|scope| {
    let synchandle = scope.spawn(move || {
      // Sync the backing store to disk every 60 seconds
      let dur = time::Duration::from_millis(60000);
      loop {
        match rx.recv_timeout(dur) {
          Err(mpsc::RecvTimeoutError::Timeout) => bsref.sync().unwrap(),
          _ => break,
        }
      }
    });

    let fshandle = scope.spawn(move || {
      let fs_mt = FuseMT::new(fs, 16);
      let options = [OsStr::new("-o"), OsStr::new("auto_unmount,default_permissions")];
      fuse_mt::mount(fs_mt, &mount, &options[..])
    });

    let ret = fshandle.join();
    tx.send(0).unwrap(); // Signal the fsync thread to die
    synchandle.join();
    ret
  })
}
