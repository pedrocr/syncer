extern crate fuse_mt;
use self::fuse_mt::*;
extern crate libc;
use self::libc::c_int;
extern crate users;
extern crate time;
use self::time::Timespec;

use std::path::Path;
use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::OsStrExt;
use std::sync::Mutex;

use crate::backingstore::*;
use crate::settings::*;
use crate::rwhashes::*;

mod entry;
pub use self::entry::*;
mod vclock;
pub use self::vclock::*;

struct Handle {
  node: NodeId,
  _flags: u32,
}

pub struct FS<'a> {
  peernum: i64,
  backing: &'a BackingStore,
  handles: RwHashes<u64,Handle>,
  handle_counter: Mutex<u64>,
}

impl<'a> FS<'a> {
  pub fn new(bs: &'a BackingStore, peernum: i64) -> Result<FS<'a>, c_int> {
    let fs = FS {
      peernum: peernum,
      backing: bs,
      handles: RwHashes::new(8),
      handle_counter: Mutex::new(0),
    };

    // Add a root node as 0 if it doesn't exist
    if !fs.backing.node_exists((0,0))? {
      let mut root = FSEntry::new(FileTypeDef::Directory, peernum);
      root.perm = 0o755;
      root.uid = users::get_current_uid();
      root.gid = users::get_current_gid();
      fs.backing.save_node((0,0), root)?;
    }
    Ok(fs)
  }

  fn with_path_optional_handle<F,T>(&self, path: &Path, fh: Option<u64>, closure: &F) -> Result<T, c_int>
    where F : Fn(&FSEntry, NodeId) -> T {
    match fh {
      Some(fh) => self.with_handle(fh, closure),
      None => self.with_path(path, closure),
    }
  }

  fn with_path<F,T>(&self, path: &Path, closure: &F) -> Result<T, c_int>
    where F : Fn(&FSEntry, NodeId) -> T {
    self.with_node(self.find_node(path)?, closure)
  }

  fn with_handle<F,T>(&self, handle: u64, closure: &F) -> Result<T, c_int>
    where F : Fn(&FSEntry, NodeId) -> T {
    let node = {
      let handles = self.handles.read(&handle);
      match handles.get(&handle) {
        Some(h) => h.node,
        None => return Err(libc::EBADF),
      }
    };
    self.with_node(node, closure)
  }

  fn with_node<F,T>(&self, node: NodeId, closure: &F) -> Result<T, c_int>
    where F : Fn(&FSEntry, NodeId) -> T {
    let entry = self.backing.get_node(node)?;
    Ok(closure(&entry, node))
  }

  fn modify_path_optional_handle<F,T>(&self, path: &Path, fh: Option<u64>, closure: &F) -> Result<T, c_int>
    where F : Fn(&mut FSEntry, NodeId) -> T {
    match fh {
      Some(fh) => self.modify_handle(fh, false, closure),
      None => self.modify_path(path, closure),
    }
  }

  fn modify_path<F,T>(&self, path: &Path, closure: &F) -> Result<T, c_int>
    where F : Fn(&mut FSEntry, NodeId) -> T {
    self.modify_node(self.find_node(path)?, false, closure)
  }

  fn modify_handle<F,T>(&self, handle: u64, cache: bool, closure: &F) -> Result<T, c_int>
    where F : Fn(&mut FSEntry, NodeId) -> T {
    let node = {
      let handles = self.handles.read(&handle);
      match handles.get(&handle) {
        Some(h) => h.node,
        None => return Err(libc::EBADF),
      }
    };
    self.modify_node(node, cache, closure)
  }

  fn modify_node<F,T>(&self, node: NodeId, cache: bool, closure: &F) -> Result<T, c_int>
    where F : Fn(&mut FSEntry, NodeId) -> T {
    let mut entry = self.backing.get_node(node)?;
    let res = closure(&mut entry, node);
    entry.clock = self::time::get_time();
    entry.vclock.increment(self.peernum);
    entry.peernum = self.peernum;
    if cache {
      self.backing.save_node_cached(node, entry)?;
    } else {
      self.backing.save_node(node, entry)?;
    }
    Ok(res)
  }

  fn find_node(&self, path: &Path) -> Result<NodeId, c_int> {
    let mut nodenum = (0, 0); // Start with the root node
    let mut iterator = path.iter();
    iterator.next(); // Skip the root as that's already nodenum 0
    for elem in iterator {
      let node = self.backing.get_node(nodenum)?;
      match node.children.get(&from_os_str(elem)?) {
        None => return Err(libc::ENOENT),
        Some(&(num,_)) => nodenum = num,
      }
    }
    Ok(nodenum)
  }

  fn create_handle(&self, handle: Handle) -> u64 {
    let count = {
      let mut counter = self.handle_counter.lock().unwrap();
      *counter += 1;
      *counter
    };
    let mut handles = self.handles.write(&count);
    handles.insert(count, handle);
    count
  }

  fn delete_handle(&self, handle: u64) -> Result<(), c_int> {
    let mut handles = self.handles.write(&handle);
    if let Some(handle) = handles.remove(&handle) {
      self.backing.sync_node(handle.node)?;
    }
    Ok(())
  }
}

impl<'a> FilesystemMT for FS<'a> {
  fn init(&self, _req:RequestInfo) -> ResultEmpty {
    Ok(())
  }

  fn destroy(&self, _req: RequestInfo) {
    match self.backing.sync_all() {
      Err(_) => eprintln!("ERROR: couldn't save on shutdown, data may have been lost"),
      _ => {},
    };
  }

  fn open(&self, _req: RequestInfo, path: &Path, flags: u32) -> ResultOpen {
    let node = self.find_node(path)?;
    let handle = self.create_handle(Handle{node: node, _flags: flags,});
    Ok((handle, flags))
  }

  fn opendir(&self, req: RequestInfo, path: &Path, flags: u32) -> ResultOpen {
    self.open(req, path, flags)
  }

  fn release(&self, _req: RequestInfo, _path: &Path, fh: u64, _flags: u32, _lock_owner: u64, _flush: bool) -> ResultEmpty {
    self.delete_handle(fh)
  }

  fn getattr(&self, _req: RequestInfo, path: &Path, fh: Option<u64>) -> ResultEntry {
    let attrs = self.with_path_optional_handle(path, fh, &(|entry, _| entry.attrs()))?;
    let time = time::get_time();
    Ok((time, attrs))
  }

  fn readdir(&self, _req: RequestInfo, _path: &Path, fh: u64) -> ResultReaddir {
    let children = self.with_handle(fh, &(|node, _| node.children()))?;
    Ok(children)
  }

  fn chmod(&self, _req: RequestInfo, path: &Path, fh: Option<u64>, mode: u32) -> ResultEmpty {
    self.modify_path_optional_handle(path, fh, &(|entry, _| {
      entry.perm = mode;
    }))
  }

  fn chown(&self, _req: RequestInfo, path: &Path, fh: Option<u64>, uid: Option<u32>, gid: Option<u32>) -> ResultEmpty {
    self.modify_path_optional_handle(path, fh, &(|entry, _| {
      if let Some(uid) = uid {entry.uid = uid};
      if let Some(gid) = gid {entry.gid = gid};
    }))
  }

  fn utimens(&self, _req: RequestInfo, path: &Path, fh: Option<u64>, atime: Option<Timespec>, mtime: Option<Timespec>) -> ResultEmpty {
    self.modify_path_optional_handle(path, fh, &(|entry, _| {
      if let Some(atime) = atime {entry.atime = atime};
      if let Some(mtime) = mtime {entry.mtime = mtime};
    }))
  }

  fn utimens_macos(&self, _req: RequestInfo, path: &Path, fh: Option<u64>, crtime: Option<Timespec>, chgtime: Option<Timespec>, bkuptime: Option<Timespec>, _flags: Option<u32>) -> ResultEmpty {
    self.modify_path_optional_handle(path, fh, &(|entry, _| {
      if let Some(crtime) = crtime {entry.crtime = crtime};
      if let Some(chgtime) = chgtime {entry.chgtime = chgtime};
      if let Some(bkuptime) = bkuptime {entry.bkuptime = bkuptime};
    }))
  }

  fn create(&self, _req: RequestInfo, parent: &Path, name: &OsStr, mode: u32, flags: u32) -> ResultCreate {
    let node = self.find_node(parent)?;
    let entry = self.with_node(node, &(|parent, _| {
      let mut e = FSEntry::new(FileTypeDef::RegularFile, self.peernum);
      e.perm = mode;
      e.gid = parent.gid;
      e.uid = parent.uid;
      e
    }))?;
    let mut created_entry = CreatedEntry {
      ttl: entry.ctime,
      attr: entry.attrs(),
      fh: 0,
      flags: entry.flags,
    };
    let newnode = self.backing.create_node(entry)?;
    created_entry.fh = self.create_handle(Handle{node: newnode, _flags: flags,});
    self.modify_node(node, false, &(|parent, _| parent.add_child(name, (newnode, FileTypeDef::RegularFile))))??;
    Ok(created_entry)
  }

  fn mkdir(&self, _req: RequestInfo, parent: &Path, name: &OsStr, mode: u32) -> ResultEntry {
    let node = self.find_node(parent)?;
    let entry = self.with_node(node, &(|parent, _| {
      let mut e = FSEntry::new(FileTypeDef::Directory, self.peernum);
      e.perm = mode;
      e.gid = parent.gid;
      e.uid = parent.uid;
      e
    }))?;
    let created_dir = (entry.ctime, entry.attrs());
    let newnode = self.backing.create_node(entry)?;
    self.modify_node(node, false, &(|parent, _| parent.add_child(name, (newnode, FileTypeDef::Directory))))??;
    Ok(created_dir)
  }

  fn symlink(&self, _req: RequestInfo, parent: &Path, name: &OsStr, target: &Path) -> ResultEntry {
    let node = self.find_node(parent)?;
    let data = target.as_os_str().as_bytes();
    let blob = self.backing.add_blob(&data)?;
    let entry = self.with_node(node, &(|parent, _| {
      let mut e = FSEntry::new(FileTypeDef::Symlink, self.peernum);
      e.blocks = vec![blob];
      e.perm = 0o777;
      e.size = data.len() as u64;
      e.gid = parent.gid;
      e.uid = parent.uid;
      e
    }))?;
    let created_symlink = (entry.ctime, entry.attrs());
    let newnode = self.backing.create_node(entry)?;
    self.modify_node(node, false, &(|parent, _| parent.add_child(name, (newnode, FileTypeDef::Symlink))))??;
    Ok(created_symlink)
  }

  fn link(&self, _req: RequestInfo, path: &Path, newparent: &Path, newname: &OsStr) -> ResultEntry {
    let childnode = self.find_node(path)?;
    let dirnode = self.find_node(newparent)?;
    let childnodeinfo = self.with_node(childnode, &(|entry, _| {
      ((entry.ctime, entry.attrs()), entry.filetype)
    }))?;
    self.modify_node(dirnode, false, &(|parent, _| parent.add_child(newname, (childnode, childnodeinfo.1))))??;
    Ok(childnodeinfo.0)
  }

  fn truncate(&self, _req: RequestInfo, path: &Path, fh: Option<u64>, size: u64) -> ResultEmpty {
    self.modify_path_optional_handle(path, fh, &(|entry, _| {
      entry.size = size;
    }))
  }

  fn write(&self, _req: RequestInfo, _path: &Path, fh: u64, offset: u64, data: Vec<u8>, _flags: u32) -> ResultWrite {
    self.modify_handle(fh, true, &(|entry, node| entry.write(node, &self.backing, offset, &data)))?
  }

  fn read(&self, _req: RequestInfo, _path: &Path, fh: u64, offset: u64, size: u32) -> ResultData {
    self.with_handle(fh, &(|entry, node| entry.read(node, &self.backing, offset, size)))?
  }

  fn readlink(&self, _req: RequestInfo, path: &Path) -> ResultData {
    self.with_path(path, &(|entry, node| entry.read(node, &self.backing, 0, BLKSIZE as u32)))?
  }

  fn rmdir(&self, _req: RequestInfo, parent: &Path, name: &OsStr) -> ResultEmpty {
    let mut path = parent.to_path_buf();
    path.push(name);

    self.with_path(&path, &(|dir, _| {
      if dir.children.len() == 0 {Ok(())} else {Err(libc::ENOTEMPTY)}
    }))??;

    self.modify_path(parent, &(|parent, _| {
      parent.remove_child(name)
    }))??;
    Ok(())
  }

  fn unlink(&self, _req: RequestInfo, parent: &Path, name: &OsStr) -> ResultEmpty {
    self.modify_path(parent, &(|parent, _| parent.remove_child(name)))??;
    Ok(())
  }

  fn rename(&self, _req: RequestInfo, parent: &Path, name: &OsStr, newparent: &Path, newname: &OsStr) -> ResultEmpty {
    let node = self.modify_path(parent, &(|parent, _| parent.remove_child(name)))??;
    self.modify_path(newparent, &(|newparent, _| newparent.add_child(newname, node)))??;
    Ok(())
  }

  fn statfs(&self, _req: RequestInfo, _path: &Path) -> ResultStatfs {
    Ok(Statfs {
      blocks: 1000000000,
      bfree:  1000000000,
      bavail: 1000000000,
      files: 0,
      ffree: 1000000000,
      bsize: 4096,
      namelen: 4096,
      frsize: 4096,
    })
  }

  fn getxattr(&self, _req: RequestInfo, path: &Path, name: &OsStr, size: u32) -> ResultXattr {
    self.with_path(path, &|entry, _| {
      let attrname = from_os_str(name)?;
      if let Some(value) = entry.xattrs.get(&attrname) {
        if size == 0 {
          Ok(Xattr::Size(value.len() as u32))
        } else {
          Ok(Xattr::Data(value.clone()))
        }
      } else {
        Err(libc::ENODATA)
      }
    })?
  }

  fn listxattr(&self, _req: RequestInfo, path: &Path, size: u32) -> ResultXattr {
    self.with_path(path, &|entry, _| {
      let mut output = Vec::<u8>::new();
      for name in entry.xattrs.keys() {
        // NOTE: .as_bytes() is UNIX only
        output.extend_from_slice(OsString::from(name).as_os_str().as_bytes());
        output.push(0);
      }

      if size == 0 {
        Ok(Xattr::Size(output.len() as u32))
      } else if size < output.len() as u32 {
        Err(libc::ERANGE)
      } else {
        Ok(Xattr::Data(output))
      }
    })?
  }

  fn setxattr(&self, _req: RequestInfo, path: &Path, name: &OsStr, value: &[u8], flags: u32, _position: u32) -> ResultEmpty {
    self.modify_path(path, &|entry, _| {
      let attrname = from_os_str(name)?;

      let has_flag = |flag| (flags as i32 & flag) != 0;
      match (has_flag(libc::XATTR_CREATE), has_flag(libc::XATTR_REPLACE)) {
        (false, false) => {},
        (false, true) => if !entry.xattrs.contains_key(&attrname) {
          return Err(libc::ENODATA);
        },
        (true, false) => if entry.xattrs.contains_key(&attrname) {
          return Err(libc::EEXIST);
        },
        (true, true) => return Err(libc::EINVAL),
      };

      entry.xattrs.insert(attrname, value.to_vec());
      Ok(())
    })?
  }

  fn removexattr(&self, _req: RequestInfo, path: &Path, name: &OsStr) -> ResultEmpty {
    self.modify_path(path, &|entry, _| {
      let attrname = from_os_str(name)?;
      match entry.xattrs.remove(&attrname) {
        Some(_) => Ok(()),
        None => Err(libc::ENODATA),
      }
    })?
  }

  fn fsync(&self, _req: RequestInfo, _path: &Path, fh: u64, _datasync: bool) -> ResultEmpty {
    self.with_handle(fh, &(|_, node| {
      self.backing.sync_node(node)?;
      self.backing.fsync_node(node)?;
      Ok(())
    }))?
  }

  fn fsyncdir(&self, req: RequestInfo, path: &Path, fh: u64, datasync: bool) -> ResultEmpty {
    self.fsync(req, path, fh, datasync)
  }

  #[cfg(target = "macos")]
  fn getxtimes(&self, _req: RequestInfo, path: &Path) -> ResultXTimes {
    self.with_path(path, &|entry, _| {
      Ok(XTimes {
          bkuptime: entry.bkuptime,
          crtime: entry.crtime,
      })
    })?
  }
}
