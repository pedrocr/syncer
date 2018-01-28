extern crate time;
extern crate fuse_mt;
extern crate libc;
extern crate users;
extern crate bincode;

use std::path::Path;
use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::OsStrExt;
use std::collections::HashMap;
use std::sync::{RwLock, Mutex};
use self::time::Timespec;
use self::libc::c_int;
use std::cmp;
use self::fuse_mt::*;
use super::blobstorage::*;
use super::metadatadb::*;
use std::io::{Error, ErrorKind};
use self::bincode::{serialize, deserialize, Infinite};

const BLKSIZE: usize = 4096;

lazy_static! {
  static ref BLKZERO: BlobHash = BlobStorage::zero(BLKSIZE);
}

pub fn run(source: &str, mount: &str) -> Result<(), Error> {
  let fs = match FS::new(source) {
    Ok(fs) => fs,
    Err(_) => return Err(Error::new(ErrorKind::Other, "Couldn't do the initial write")),
  };
  let fs_mt = FuseMT::new(fs, 16);
  let options = [OsStr::new("-o"), OsStr::new("auto_unmount,default_permissions")];
  fuse_mt::mount(fs_mt, &mount, &options[..])
}

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

#[derive(Serialize, Deserialize)]
struct FSEntry {
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
  size: u64,
  blocks: Vec<BlobHash>,
  children: HashMap<OsString, (u64, FileTypeDef)>,
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
      size: 0,
      blocks: Vec::new(),
      children: HashMap::new(),
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
      crtime: self.ctime,
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
        name: key.clone(),
        kind: val.1.to_filetype(),
      });
    }
    out
  }

  fn add_child(&mut self, name: &OsStr, node: (u64, FileTypeDef)) {
    self.children.insert(name.to_os_string(), node);
  }

  fn remove_child(&mut self, name: &OsStr) -> Result<(u64, FileTypeDef), c_int> {
    match self.children.remove(name) {
      None => Err(libc::ENOENT),
      Some(c) => Ok(c),
    }
  }

  fn write(&mut self, bs: &BlobStorage, offset: u64, data: &[u8]) -> Result<u32, c_int> {
    self.size = cmp::max(self.size, offset + data.len() as u64);
    let total_needed_blocks = (self.size as usize + BLKSIZE - 1) / BLKSIZE;
    if total_needed_blocks > self.blocks.len() {
      self.blocks.resize(total_needed_blocks, *BLKZERO);
    }

    let start = offset as usize;
    let end = start + data.len();
    let mut written = 0;
    let startblock = start/BLKSIZE;
    let endblock = (end + BLKSIZE - 1)/BLKSIZE;
    for (i,block) in self.blocks[startblock..endblock].iter_mut().enumerate() {
      let i = i+startblock;
      let bstart = cmp::max(start, i*BLKSIZE);
      let bend = cmp::min(end, (i+1)*BLKSIZE);
      let bsize = bend - bstart;
      let boffset = bstart - i*BLKSIZE;
      let newblock = try!(bs.write(block, boffset, &data[written..written+bsize]));
      block.copy_from_slice(&newblock);
      written += bsize;
    }
    assert!(written == data.len());
    self.mtime = self::time::get_time();
    Ok(written as u32)
  }

  fn read(&self, bs: &BlobStorage, offset: u64, size: u32) -> Result<Vec<u8>, c_int> {
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
      data[written..written+bsize].copy_from_slice(&try!(bs.read(block, boffset, bsize)));
      written += bsize;
    }
    assert!(written == data.len());
    Ok(data)
  }
}

struct Handle {
  node: u64,
  _flags: u32,
}

pub struct FS {
  blob_storage: BlobStorage,
  nodes: MetadataDB,
  node_counter: Mutex<u64>,
  handles: RwLock<HashMap<u64,Handle>>,
  handle_counter: Mutex<u64>,
}

impl FS {
  pub fn new(source: &str) -> Result<FS, c_int> {
    let bs = BlobStorage::new(source);
    let empty_block = [0 as u8; BLKSIZE];
    try!(bs.add_blob(&empty_block));
    let fs = FS {
      blob_storage: bs,
      nodes: MetadataDB::new(source),
      node_counter: Mutex::new(0),
      handles: RwLock::new(HashMap::new()),
      handle_counter: Mutex::new(0),
    };

    // Add a root node as 0 if it doesn't exist
    match fs.get_entry(0) {
      Ok(_) => {},
      Err(_) => {
        let mut root = FSEntry::new(FileTypeDef::Directory);
        root.perm = 0o755;
        root.uid = users::get_current_uid();
        root.gid = users::get_current_gid();
        try!(fs.save_node(0, &root));
      }
    }
    Ok(fs)
  }

  fn with_path_optional_handle<F,T>(&self, path: &Path, fh: Option<u64>, closure: &F) -> Result<T, c_int>
    where F : Fn(&FSEntry) -> T {
    match fh {
      Some(fh) => self.with_handle(fh, closure),
      None => self.with_path(path, closure),
    }
  }

  fn with_path<F,T>(&self, path: &Path, closure: &F) -> Result<T, c_int>
    where F : Fn(&FSEntry) -> T {
    self.with_node(try!(self.find_node(path)), closure)
  }

  fn with_handle<F,T>(&self, handle: u64, closure: &F) -> Result<T, c_int>
    where F : Fn(&FSEntry) -> T {
    let node = {
      let handles = self.handles.read().unwrap();
      match handles.get(&handle) {
        Some(h) => h.node,
        None => return Err(libc::EBADF),
      }
    };
    self.with_node(node, closure)
  }

  fn get_entry(&self, node: u64) -> Result<FSEntry, c_int> {
    let hash = try!(self.nodes.get(node));
    let buffer = try!(self.blob_storage.read_all(&hash));
    Ok(deserialize(&buffer[..]).unwrap())
  }

  fn with_node<F,T>(&self, node: u64, closure: &F) -> Result<T, c_int>
    where F : Fn(&FSEntry) -> T {
    let entry = try!(self.get_entry(node));
    Ok(closure(&entry))
  }

  fn modify_path_optional_handle<F,T>(&self, path: &Path, fh: Option<u64>, closure: &F) -> Result<T, c_int>
    where F : Fn(&mut FSEntry) -> T {
    match fh {
      Some(fh) => self.modify_handle(fh, closure),
      None => self.modify_path(path, closure),
    }
  }

  fn modify_path<F,T>(&self, path: &Path, closure: &F) -> Result<T, c_int>
    where F : Fn(&mut FSEntry) -> T {
    self.modify_node(try!(self.find_node(path)), closure)
  }

  fn modify_handle<F,T>(&self, handle: u64, closure: &F) -> Result<T, c_int>
    where F : Fn(&mut FSEntry) -> T {
    let node = {
      let handles = self.handles.read().unwrap();
      match handles.get(&handle) {
        Some(h) => h.node,
        None => return Err(libc::EBADF),
      }
    };
    self.modify_node(node, closure)
  }

  fn save_node(&self, node: u64, entry: &FSEntry) -> Result<(), c_int> {
    let encoded: Vec<u8> = serialize(entry, Infinite).unwrap();
    let hash = try!(self.blob_storage.add_blob(&encoded));
    try!(self.nodes.set(node, &hash));
    Ok(())
  }

  fn modify_node<F,T>(&self, node: u64, closure: &F) -> Result<T, c_int>
    where F : Fn(&mut FSEntry) -> T {
    let mut entry = try!(self.get_entry(node));
    let res = closure(&mut entry);
    try!(self.save_node(node, &entry));
    Ok(res)
  }

  fn find_node(&self, path: &Path) -> Result<u64, c_int> {
    let mut nodenum = 0; // Start with the root node
    let mut iterator = path.iter();
    iterator.next(); // Skip the root as that's already nodenum 0
    for elem in iterator {
      let node = try!(self.get_entry(nodenum));
      match node.children.get(elem) {
        None => return Err(libc::ENOENT),
        Some(&(num,_)) => nodenum = num,
      }
    }
    Ok(nodenum)
  }

  fn create_node(&self, entry: FSEntry) -> Result<u64, c_int> {
    let node = {
      let mut counter = self.node_counter.lock().unwrap();
      *counter += 1;
      *counter
    };
    try!(self.save_node(node, &entry));
    Ok(node)
  }

  fn create_handle(&self, handle: Handle) -> u64 {
    let count = {
      let mut counter = self.handle_counter.lock().unwrap();
      *counter += 1;
      *counter
    };
    let mut handles = self.handles.write().unwrap();
    handles.insert(count, handle);
    count
  }

  fn delete_handle(&self, handle: u64) {
    let mut handles = self.handles.write().unwrap();
    handles.remove(&handle);
  }
}

impl FilesystemMT for FS {
  fn init(&self, _req:RequestInfo) -> ResultEmpty {
    Ok(())
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
    self.delete_handle(fh);
    Ok(())
  }

  fn getattr(&self, _req: RequestInfo, path: &Path, fh: Option<u64>) -> ResultEntry {
    let attrs = try!(self.with_path_optional_handle(path, fh, &(|entry| entry.attrs())));
    let time = time::get_time();
    Ok((time, attrs))
  }

  fn readdir(&self, _req: RequestInfo, _path: &Path, fh: u64) -> ResultReaddir {
    let children = try!(self.with_handle(fh, &(|node| node.children())));
    Ok(children)
  }

  fn chmod(&self, _req: RequestInfo, path: &Path, fh: Option<u64>, mode: u32) -> ResultEmpty {
    self.modify_path_optional_handle(path, fh, &(|entry| {
      entry.perm = mode;
    }))
  }

  fn chown(&self, _req: RequestInfo, path: &Path, fh: Option<u64>, uid: Option<u32>, gid: Option<u32>) -> ResultEmpty {
    self.modify_path_optional_handle(path, fh, &(|entry| {
      if let Some(uid) = uid {entry.uid = uid};
      if let Some(gid) = gid {entry.gid = gid};
    }))
  }

  fn utimens(&self, _req: RequestInfo, path: &Path, fh: Option<u64>, atime: Option<Timespec>, mtime: Option<Timespec>) -> ResultEmpty {
    self.modify_path_optional_handle(path, fh, &(|entry| {
      if let Some(atime) = atime {entry.atime = atime};
      if let Some(mtime) = mtime {entry.mtime = mtime};
    }))
  }

  fn create(&self, _req: RequestInfo, parent: &Path, name: &OsStr, mode: u32, flags: u32) -> ResultCreate {
    let node = try!(self.find_node(parent));
    let entry = try!(self.with_node(node, &(|parent| {
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
    let newnode = try!(self.create_node(entry));
    created_entry.fh = self.create_handle(Handle{node: newnode, _flags: flags,});
    try!(self.modify_node(node, &(|parent| parent.add_child(name, (newnode, FileTypeDef::RegularFile)))));
    Ok(created_entry)
  }

  fn mkdir(&self, _req: RequestInfo, parent: &Path, name: &OsStr, mode: u32) -> ResultEntry {
    let node = try!(self.find_node(parent));
    let entry = try!(self.with_node(node, &(|parent| {
      let mut e = FSEntry::new(FileTypeDef::Directory);
      e.perm = mode;
      e.gid = parent.gid;
      e.uid = parent.uid;
      e
    })));
    let created_dir = (entry.ctime, entry.attrs());
    let newnode = try!(self.create_node(entry));
    try!(self.modify_node(node, &(|parent| parent.add_child(name, (newnode, FileTypeDef::Directory)))));
    Ok(created_dir)
  }

  fn symlink(&self, _req: RequestInfo, parent: &Path, name: &OsStr, target: &Path) -> ResultEntry {
    let node = try!(self.find_node(parent));
    let data = target.as_os_str().as_bytes();
    let mut blockdata = [0; BLKSIZE];
    blockdata[0..data.len()].copy_from_slice(data);
    let blob = try!(self.blob_storage.add_blob(&blockdata));
    let entry = try!(self.with_node(node, &(|parent| {
      let mut e = FSEntry::new(FileTypeDef::Symlink);
      e.blocks = vec![blob];
      e.perm = 0o777;
      e.size = data.len() as u64;
      e.gid = parent.gid;
      e.uid = parent.uid;
      e
    })));
    let created_symlink = (entry.ctime, entry.attrs());
    let newnode = try!(self.create_node(entry));
    try!(self.modify_node(node, &(|parent| parent.add_child(name, (newnode, FileTypeDef::Symlink)))));
    Ok(created_symlink)
  }

  fn link(&self, _req: RequestInfo, path: &Path, newparent: &Path, newname: &OsStr) -> ResultEntry {
    let childnode = try!(self.find_node(path));
    let dirnode = try!(self.find_node(newparent));
    let childnodeinfo = try!(self.with_node(childnode, &(|entry| {
      ((entry.ctime, entry.attrs()), entry.filetype)
    })));
    try!(self.modify_node(dirnode, &(|parent| parent.add_child(newname, (childnode, childnodeinfo.1)))));
    Ok(childnodeinfo.0)
  }

  fn truncate(&self, _req: RequestInfo, path: &Path, fh: Option<u64>, size: u64) -> ResultEmpty {
    self.modify_path_optional_handle(path, fh, &(|entry| {
      entry.size = size;
    }))
  }

  fn write(&self, _req: RequestInfo, _path: &Path, fh: u64, offset: u64, data: Vec<u8>, _flags: u32) -> ResultWrite {
    try!(self.modify_handle(fh, &(|entry| entry.write(&self.blob_storage, offset, &data))))
  }

  fn read(&self, _req: RequestInfo, _path: &Path, fh: u64, offset: u64, size: u32) -> ResultData {
    try!(self.with_handle(fh, &(|entry| entry.read(&self.blob_storage, offset, size))))
  }

  fn readlink(&self, _req: RequestInfo, path: &Path) -> ResultData {
    try!(self.with_path(path, &(|entry| entry.read(&self.blob_storage, 0, BLKSIZE as u32))))
  }

  fn rmdir(&self, _req: RequestInfo, parent: &Path, name: &OsStr) -> ResultEmpty {
    let mut path = parent.to_path_buf();
    path.push(name);

    try!(try!(self.with_path(&path, &(|dir| {
      if dir.children.len() == 0 {Ok(())} else {Err(libc::ENOTEMPTY)}
    }))));

    try!(try!(self.modify_path(parent, &(|parent| {
      parent.remove_child(name)
    }))));
    Ok(())
  }

  fn unlink(&self, _req: RequestInfo, parent: &Path, name: &OsStr) -> ResultEmpty {
    try!(try!(self.modify_path(parent, &(|parent| parent.remove_child(name)))));
    Ok(())
  }

  fn rename(&self, _req: RequestInfo, parent: &Path, name: &OsStr, newparent: &Path, newname: &OsStr) -> ResultEmpty {
    let node = try!(try!(self.modify_path(parent, &(|parent| parent.remove_child(name)))));
    try!(self.modify_path(newparent, &(|newparent| newparent.add_child(newname, node))));
    Ok(())
  }
}

