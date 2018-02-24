extern crate time;
extern crate fuse_mt;
extern crate libc;
extern crate users;

use std::path::Path;
use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::OsStrExt;
use std::collections::HashMap;
use std::sync::Mutex;
use self::time::Timespec;
use self::libc::c_int;
use std::cmp;
use self::fuse_mt::*;
use backingstore::*;
use settings::*;
use rwhashes::*;

#[derive(Serialize, Deserialize)]
#[serde(remote = "Timespec")]
#[allow(dead_code)]
struct TimespecDef {
  sec: i64,
  nsec: i32,
}

#[derive(Copy, Clone, PartialEq, Serialize, Deserialize)]
enum FileTypeDef {
  NamedPipe,
  CharDevice,
  BlockDevice,
  Directory,
  RegularFile,
  Symlink,
  Socket,
}

impl FileTypeDef {
  fn to_filetype(&self) -> FileType {
    match *self {
      FileTypeDef::NamedPipe => FileType::NamedPipe,
      FileTypeDef::CharDevice => FileType::CharDevice,
      FileTypeDef::BlockDevice => FileType::BlockDevice,
      FileTypeDef::Directory => FileType::Directory,
      FileTypeDef::RegularFile => FileType::RegularFile,
      FileTypeDef::Symlink => FileType::Symlink,
      FileTypeDef::Socket => FileType::Socket,
    }
  }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct FSEntry {
  filetype: FileTypeDef,
  perm: u32,
  uid: u32,
  gid: u32,
  flags: u32,
  rdev: u32,
  #[serde(with = "TimespecDef")]
  atime: Timespec,
  #[serde(with = "TimespecDef")]
  mtime: Timespec,
  #[serde(with = "TimespecDef")]
  ctime: Timespec,
  #[serde(with = "TimespecDef")]
  crtime: Timespec,
  #[serde(with = "TimespecDef")]
  chgtime: Timespec,
  #[serde(with = "TimespecDef")]
  bkuptime: Timespec,
  size: u64,
  blocks: Vec<BlobHash>,
  children: HashMap<String, (u64, FileTypeDef)>,
  xattrs: HashMap<String, Vec<u8>>,
}

fn from_os_str(ostr: &OsStr) -> Result<String, c_int> {
  ostr.to_os_string().into_string().or_else(|_| Err(libc::EIO))
}

impl FSEntry {
  fn new(filetype: FileTypeDef) -> FSEntry {
    let time = self::time::get_time();

    FSEntry {
      filetype: filetype,
      perm: 0,
      uid: 0,
      gid: 0,
      flags: 0,
      rdev: 0,
      atime: time,
      mtime: time,
      ctime: time,
      crtime: time,
      chgtime: time,
      bkuptime: time,
      size: 0,
      blocks: Vec::new(),
      children: HashMap::new(),
      xattrs: HashMap::new(),
    }
  }

  fn attrs(&self) -> FileAttr {
    let blocks = (self.size + 512 -1)/ 512;

    FileAttr {
      size: self.size,
      blocks,
      atime: self.atime,
      mtime: self.mtime,
      ctime: self.ctime,
      crtime: self.crtime,
      kind: self.filetype.to_filetype(),
      perm: self.perm as u16,
      nlink: 1,
      uid: self.uid,
      gid: self.gid,
      rdev: self.rdev,
      flags: self.flags,
    }
  }

  fn children(&self) -> Vec<DirectoryEntry> {
    assert!(self.filetype == FileTypeDef::Directory);
    let mut out = Vec::new();
    out.push(DirectoryEntry{name: OsString::from("."), kind: FileType::Directory});
    out.push(DirectoryEntry{name: OsString::from(".."), kind: FileType::Directory});
    for (key, val) in self.children.iter() {
      out.push(DirectoryEntry{
        name: key.clone().into(),
        kind: val.1.to_filetype(),
      });
    }
    out
  }

  fn add_child(&mut self, name: &OsStr, node: (u64, FileTypeDef)) -> Result<(), c_int> {
    self.children.insert(try!(from_os_str(name)), node);
    Ok(())
  }

  fn remove_child(&mut self, name: &OsStr) -> Result<(u64, FileTypeDef), c_int> {
    match self.children.remove(&try!(from_os_str(name))) {
      None => Err(libc::ENOENT),
      Some(c) => Ok(c),
    }
  }

  fn write(&mut self, node: u64, bs: &BackingStore, offset: u64, data: &[u8]) -> Result<u32, c_int> {
    self.size = cmp::max(self.size, offset + data.len() as u64);
    let total_needed_blocks = (self.size as usize + BLKSIZE - 1) / BLKSIZE;
    if total_needed_blocks > self.blocks.len() {
      self.blocks.resize(total_needed_blocks, bs.blob_zero());
    }

    let start = offset as usize;
    let end = start + data.len();
    let mut written = 0;
    let startblock = start/BLKSIZE;
    let endblock = (end + BLKSIZE - 1)/BLKSIZE;
    for i in startblock..endblock {
      let block = &self.blocks[i];
      let bstart = cmp::max(start, i*BLKSIZE);
      let bend = cmp::min(end, (i+1)*BLKSIZE);
      let bsize = bend - bstart;
      let boffset = bstart - i*BLKSIZE;
      try!(bs.write(node, i, block, boffset, &data[written..written+bsize]));
      written += bsize;
    }
    assert!(written == data.len());
    self.mtime = self::time::get_time();
    Ok(written as u32)
  }

  fn read(&self, node: u64, bs: &BackingStore, offset: u64, size: u32) -> Result<Vec<u8>, c_int> {
    if offset >= self.size {
      // We're asking for an out of bounds offset
      return Ok(Vec::new())
    }

    let start = offset as usize;
    let end = cmp::min(start + (size as usize), self.size as usize);
    let mut data = vec![0; end - start];
    let mut written = 0;
    let startblock = start/BLKSIZE;
    let endblock = (end + BLKSIZE - 1)/BLKSIZE;
    for i in startblock..endblock {
      let block = &self.blocks[i];
      let bstart = cmp::max(start, i*BLKSIZE);
      let bend = cmp::min(end, (i+1)*BLKSIZE);
      let bsize = bend - bstart;
      let boffset = bstart - i*BLKSIZE;
      data[written..written+bsize].copy_from_slice(&try!(bs.read(node, i, block, boffset, bsize)));
      written += bsize;
    }
    assert!(written == data.len());
    Ok(data)
  }

  pub fn set_block(&mut self, i: usize, hash: BlobHash) {
    self.blocks[i].copy_from_slice(&hash);
  }

  pub fn get_blocks(&self) -> &Vec<BlobHash> {
    &self.blocks
  }
}

struct Handle {
  node: u64,
  _flags: u32,
}

pub struct FS<'a> {
  backing: &'a BackingStore,
  handles: RwHashes<u64,Handle>,
  handle_counter: Mutex<u64>,
}

impl<'a> FS<'a> {
  pub fn new(bs: &'a BackingStore) -> Result<FS<'a>, c_int> {
    let fs = FS {
      backing: bs,
      handles: RwHashes::new(8),
      handle_counter: Mutex::new(0),
    };

    // Add a root node as 0 if it doesn't exist
    if !try!(fs.backing.node_exists(0)) {
      let mut root = FSEntry::new(FileTypeDef::Directory);
      root.perm = 0o755;
      root.uid = users::get_current_uid();
      root.gid = users::get_current_gid();
      try!(fs.backing.save_node(0, root));
    }
    Ok(fs)
  }

  fn with_path_optional_handle<F,T>(&self, path: &Path, fh: Option<u64>, closure: &F) -> Result<T, c_int>
    where F : Fn(&FSEntry, u64) -> T {
    match fh {
      Some(fh) => self.with_handle(fh, closure),
      None => self.with_path(path, closure),
    }
  }

  fn with_path<F,T>(&self, path: &Path, closure: &F) -> Result<T, c_int>
    where F : Fn(&FSEntry, u64) -> T {
    self.with_node(try!(self.find_node(path)), closure)
  }

  fn with_handle<F,T>(&self, handle: u64, closure: &F) -> Result<T, c_int>
    where F : Fn(&FSEntry, u64) -> T {
    let node = {
      let handles = self.handles.read(&handle);
      match handles.get(&handle) {
        Some(h) => h.node,
        None => return Err(libc::EBADF),
      }
    };
    self.with_node(node, closure)
  }

  fn with_node<F,T>(&self, node: u64, closure: &F) -> Result<T, c_int>
    where F : Fn(&FSEntry, u64) -> T {
    let entry = try!(self.backing.get_node(node));
    Ok(closure(&entry, node))
  }

  fn modify_path_optional_handle<F,T>(&self, path: &Path, fh: Option<u64>, closure: &F) -> Result<T, c_int>
    where F : Fn(&mut FSEntry, u64) -> T {
    match fh {
      Some(fh) => self.modify_handle(fh, false, closure),
      None => self.modify_path(path, closure),
    }
  }

  fn modify_path<F,T>(&self, path: &Path, closure: &F) -> Result<T, c_int>
    where F : Fn(&mut FSEntry, u64) -> T {
    self.modify_node(try!(self.find_node(path)), false, closure)
  }

  fn modify_handle<F,T>(&self, handle: u64, cache: bool, closure: &F) -> Result<T, c_int>
    where F : Fn(&mut FSEntry, u64) -> T {
    let node = {
      let handles = self.handles.read(&handle);
      match handles.get(&handle) {
        Some(h) => h.node,
        None => return Err(libc::EBADF),
      }
    };
    self.modify_node(node, cache, closure)
  }

  fn modify_node<F,T>(&self, node: u64, cache: bool, closure: &F) -> Result<T, c_int>
    where F : Fn(&mut FSEntry, u64) -> T {
    let mut entry = try!(self.backing.get_node(node));
    let res = closure(&mut entry, node);
    if cache {
      try!(self.backing.save_node_cached(node, entry));
    } else {
      try!(self.backing.save_node(node, entry));
    }
    Ok(res)
  }

  fn find_node(&self, path: &Path) -> Result<u64, c_int> {
    let mut nodenum = 0; // Start with the root node
    let mut iterator = path.iter();
    iterator.next(); // Skip the root as that's already nodenum 0
    for elem in iterator {
      let node = try!(self.backing.get_node(nodenum));
      match node.children.get(&try!(from_os_str(elem))) {
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
      try!(self.backing.sync_node(handle.node));
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
    let node = try!(self.find_node(path));
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
    let attrs = try!(self.with_path_optional_handle(path, fh, &(|entry, _| entry.attrs())));
    let time = time::get_time();
    Ok((time, attrs))
  }

  fn readdir(&self, _req: RequestInfo, _path: &Path, fh: u64) -> ResultReaddir {
    let children = try!(self.with_handle(fh, &(|node, _| node.children())));
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
    let node = try!(self.find_node(parent));
    let entry = try!(self.with_node(node, &(|parent, _| {
      let mut e = FSEntry::new(FileTypeDef::RegularFile);
      e.perm = mode;
      e.gid = parent.gid;
      e.uid = parent.uid;
      e
    })));
    let mut created_entry = CreatedEntry {
      ttl: entry.ctime,
      attr: entry.attrs(),
      fh: 0,
      flags: entry.flags,
    };
    let newnode = try!(self.backing.create_node(entry));
    created_entry.fh = self.create_handle(Handle{node: newnode, _flags: flags,});
    try!(try!(self.modify_node(node, false, &(|parent, _| parent.add_child(name, (newnode, FileTypeDef::RegularFile))))));
    Ok(created_entry)
  }

  fn mkdir(&self, _req: RequestInfo, parent: &Path, name: &OsStr, mode: u32) -> ResultEntry {
    let node = try!(self.find_node(parent));
    let entry = try!(self.with_node(node, &(|parent, _| {
      let mut e = FSEntry::new(FileTypeDef::Directory);
      e.perm = mode;
      e.gid = parent.gid;
      e.uid = parent.uid;
      e
    })));
    let created_dir = (entry.ctime, entry.attrs());
    let newnode = try!(self.backing.create_node(entry));
    try!(try!(self.modify_node(node, false, &(|parent, _| parent.add_child(name, (newnode, FileTypeDef::Directory))))));
    Ok(created_dir)
  }

  fn symlink(&self, _req: RequestInfo, parent: &Path, name: &OsStr, target: &Path) -> ResultEntry {
    let node = try!(self.find_node(parent));
    let data = target.as_os_str().as_bytes();
    let blob = try!(self.backing.add_blob(&data));
    let entry = try!(self.with_node(node, &(|parent, _| {
      let mut e = FSEntry::new(FileTypeDef::Symlink);
      e.blocks = vec![blob];
      e.perm = 0o777;
      e.size = data.len() as u64;
      e.gid = parent.gid;
      e.uid = parent.uid;
      e
    })));
    let created_symlink = (entry.ctime, entry.attrs());
    let newnode = try!(self.backing.create_node(entry));
    try!(try!(self.modify_node(node, false, &(|parent, _| parent.add_child(name, (newnode, FileTypeDef::Symlink))))));
    Ok(created_symlink)
  }

  fn link(&self, _req: RequestInfo, path: &Path, newparent: &Path, newname: &OsStr) -> ResultEntry {
    let childnode = try!(self.find_node(path));
    let dirnode = try!(self.find_node(newparent));
    let childnodeinfo = try!(self.with_node(childnode, &(|entry, _| {
      ((entry.ctime, entry.attrs()), entry.filetype)
    })));
    try!(try!(self.modify_node(dirnode, false, &(|parent, _| parent.add_child(newname, (childnode, childnodeinfo.1))))));
    Ok(childnodeinfo.0)
  }

  fn truncate(&self, _req: RequestInfo, path: &Path, fh: Option<u64>, size: u64) -> ResultEmpty {
    self.modify_path_optional_handle(path, fh, &(|entry, _| {
      entry.size = size;
    }))
  }

  fn write(&self, _req: RequestInfo, _path: &Path, fh: u64, offset: u64, data: Vec<u8>, _flags: u32) -> ResultWrite {
    try!(self.modify_handle(fh, true, &(|entry, node| entry.write(node, &self.backing, offset, &data))))
  }

  fn read(&self, _req: RequestInfo, _path: &Path, fh: u64, offset: u64, size: u32) -> ResultData {
    try!(self.with_handle(fh, &(|entry, node| entry.read(node, &self.backing, offset, size))))
  }

  fn readlink(&self, _req: RequestInfo, path: &Path) -> ResultData {
    try!(self.with_path(path, &(|entry, node| entry.read(node, &self.backing, 0, BLKSIZE as u32))))
  }

  fn rmdir(&self, _req: RequestInfo, parent: &Path, name: &OsStr) -> ResultEmpty {
    let mut path = parent.to_path_buf();
    path.push(name);

    try!(try!(self.with_path(&path, &(|dir, _| {
      if dir.children.len() == 0 {Ok(())} else {Err(libc::ENOTEMPTY)}
    }))));

    try!(try!(self.modify_path(parent, &(|parent, _| {
      parent.remove_child(name)
    }))));
    Ok(())
  }

  fn unlink(&self, _req: RequestInfo, parent: &Path, name: &OsStr) -> ResultEmpty {
    try!(try!(self.modify_path(parent, &(|parent, _| parent.remove_child(name)))));
    Ok(())
  }

  fn rename(&self, _req: RequestInfo, parent: &Path, name: &OsStr, newparent: &Path, newname: &OsStr) -> ResultEmpty {
    let node = try!(try!(self.modify_path(parent, &(|parent, _| parent.remove_child(name)))));
    try!(try!(self.modify_path(newparent, &(|newparent, _| newparent.add_child(newname, node)))));
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
    try!(self.with_path(path, &|entry, _| {
      let attrname = try!(from_os_str(name));
      if let Some(value) = entry.xattrs.get(&attrname) {
        if size == 0 {
          Ok(Xattr::Size(value.len() as u32))
        } else {
          Ok(Xattr::Data(value.clone()))
        }
      } else {
        Err(libc::ENOATTR)
      }
    }))
  }

  fn listxattr(&self, _req: RequestInfo, path: &Path, size: u32) -> ResultXattr {
    try!(self.with_path(path, &|entry, _| {
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
    }))
  }

  fn setxattr(&self, _req: RequestInfo, path: &Path, name: &OsStr, value: &[u8], flags: u32, _position: u32) -> ResultEmpty {
    try!(self.modify_path(path, &|entry, _| {
      let attrname = try!(from_os_str(name));

      let has_flag = |flag| (flags as i32 & flag) != 0;
      match (has_flag(libc::XATTR_CREATE), has_flag(libc::XATTR_REPLACE)) {
        (false, false) => {},
        (false, true) => if !entry.xattrs.contains_key(&attrname) {
          return Err(libc::ENOATTR);
        },
        (true, false) => if entry.xattrs.contains_key(&attrname) {
          return Err(libc::EEXIST);
        },
        (true, true) => return Err(libc::EINVAL),
      };

      entry.xattrs.insert(attrname, value.to_vec());
      Ok(())
    }))
  }

  fn removexattr(&self, _req: RequestInfo, path: &Path, name: &OsStr) -> ResultEmpty {
    try!(self.modify_path(path, &|entry, _| {
      let attrname = try!(from_os_str(name));
      match entry.xattrs.remove(&attrname) {
        Some(_) => Ok(()),
        None => Err(libc::ENOATTR),
      }
    }))
  }

  fn fsync(&self, _req: RequestInfo, _path: &Path, fh: u64, _datasync: bool) -> ResultEmpty {
    try!(self.with_handle(fh, &(|_, node| {
      try!(self.backing.sync_node(node));
      try!(self.backing.fsync_node(node));
      Ok(())
    })))
  }

  fn fsyncdir(&self, req: RequestInfo, path: &Path, fh: u64, datasync: bool) -> ResultEmpty {
    self.fsync(req, path, fh, datasync)
  }
}

