extern crate time;
extern crate fuse_mt;
extern crate libc;

use fuse_mt::*;
use std::path::{Path,PathBuf};
use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::OsStrExt;
use std::collections::{BTreeMap, HashMap};
use std::sync::{RwLock, Mutex};
use time::Timespec;
use libc::c_int;
use std::cmp;

#[derive(Clone)]
struct FSBlock {
  data: [u8; 4096],
}

impl FSBlock {
  fn new() -> FSBlock {
    FSBlock {
      data: [0; 4096],
    }
  }
}

struct FSEntry {
  filetype: FileType,
  perm: u32,
  uid: u32,
  gid: u32,
  flags: u32,
  rdev: u32,
  atime: Timespec,
  mtime: Timespec,
  ctime: Timespec,
  size: u64,
  blocks: Vec<FSBlock>,
}

impl FSEntry {
  fn new(filetype: FileType) -> FSEntry {
    let time = time::get_time();

    FSEntry {
      filetype,
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
      kind: self.filetype,
      perm: self.perm as u16,
      nlink: 0,
      uid: self.uid,
      gid: self.gid,
      rdev: self.rdev,
      flags: self.flags,      
    }
  }

  fn write(&mut self, offset: u64, data: &[u8]) {
    self.size = cmp::max(self.size, offset + data.len() as u64);
    let total_needed_blocks = ((self.size + 4096 - 1) / 4096) as usize;
    if total_needed_blocks > self.blocks.len() {
      self.blocks.resize(total_needed_blocks, FSBlock::new());
    }

    let start = offset as usize;
    let end = start + data.len();
    let mut written = 0;
    let startblock = start/4096;
    let endblock = (end + 4096 - 1)/4096;
    for (i,block) in self.blocks[startblock..endblock].iter_mut().enumerate() {
      let i = i+startblock;
      let bstart = cmp::max(start, i*4096);
      let bend = cmp::min(end, (i+1)*4096);
      let bsize = bend - bstart;
      let boffset = bstart - i*4096;
      block.data[boffset..boffset+bsize].copy_from_slice(&data[written..written+bsize]);
      written += bsize;
    }
    assert!(written == data.len());
  }

  fn read(&self, offset: u64, size: u32) -> Vec<u8> {
    let start = offset as usize;
    let end = cmp::min(start + (size as usize), self.size as usize);
    let mut data = vec![0; end - start];
    let mut written = 0;
    let startblock = start/4096;
    let endblock = (end + 4096 - 1)/4096;
    for i in startblock..endblock {
      let block = &self.blocks[i];
      let bstart = cmp::max(start, i*4096);
      let bend = cmp::min(end, (i+1)*4096 - 1);
      let bsize = bend - bstart;
      let boffset = bstart - i*4096;
      data[written..written+bsize].copy_from_slice(&block.data[boffset..boffset+bsize]);
      written += bsize;
    }
    assert!(written == data.len());
    data
  }
}

struct Handle {
  node: u64,
  _flags: u32,
}

struct FS {
  entries: RwLock<BTreeMap<PathBuf,(u64,FileType)>>,
  nodes: RwLock<HashMap<u64,FSEntry>>,
  node_counter: Mutex<u64>,
  handles: RwLock<HashMap<u64,Handle>>,
  handle_counter: Mutex<u64>,
}

impl FS {
  fn new() -> FS {
    FS {
      entries: RwLock::new(BTreeMap::new()),
      nodes: RwLock::new(HashMap::new()),
      node_counter: Mutex::new(0),
      handles: RwLock::new(HashMap::new()),
      handle_counter: Mutex::new(0),
    }
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
    let node = {
      let entries = self.entries.read().unwrap();
      match entries.get(&(path.to_path_buf())) {
        Some(e) => e.0,
        None => return Err(libc::ENOENT),
      }
    };
    self.with_node(node, closure)
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

  fn with_node<F,T>(&self, node: u64, closure: &F) -> Result<T, c_int>
    where F : Fn(&FSEntry) -> T {
    let nodes = self.nodes.read().unwrap();
    match nodes.get(&node) {
      Some(e) => Ok(closure(e)),
      None => return Err(libc::ENOENT),
    }
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
    let node = {
      let entries = self.entries.read().unwrap();
      match entries.get(&(path.to_path_buf())) {
        Some(e) => e.0,
        None => return Err(libc::ENOENT),
      }
    };
    self.modify_node(node, closure)
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

  fn modify_node<F,T>(&self, node: u64, closure: &F) -> Result<T, c_int>
    where F : Fn(&mut FSEntry) -> T {
    let mut nodes = self.nodes.write().unwrap();
    Ok(match nodes.get_mut(&node) {
      Some(mut entry) => closure(&mut entry),
      None => return Err(libc::ENOENT),
    })
  }

  fn get_children(&self, path: &Path) -> Vec<(PathBuf, FileType)> {
    // List all the children of a given path by iterating the BTreeMap
    // This isn't particularly efficient for directories with a lot of children
    // directories because the sorting order results in a depth first search. Fixing
    // the sorting order would probably fix that.

    let mut children = Vec::new();
    let entries = self.entries.read().unwrap();

    for child in entries.range(path.to_path_buf()..) {
      // It's the path itself, skip
      if child.0 == path { continue }
      // This is not a child of the dir
      if child.0.parent().unwrap() != path { continue }
      // We're past the dir itself
      if !child.0.starts_with(path) { break }

      children.push((child.0.clone(), (child.1).1));
    }

    children
  }

  fn remove_entry(&self, path: &Path) -> Option<(u64,FileType)> {
    let mut entries = self.entries.write().unwrap();
    entries.remove(path)
  }

  fn find_node(&self, path: &Path) -> Option<(u64,FileType)> {
    let entries = self.entries.read().unwrap();
    match entries.get(path) {
      Some(&e) => Some(e),
      None => None,
    }
  }

  fn link_entry(&self, path: PathBuf, node: (u64, FileType)) {
    let mut entries = self.entries.write().unwrap();
    entries.insert(path, node);
  }

  fn insert_entry(&self, path: PathBuf, entry: FSEntry) {
    let filetype = entry.filetype;
    let node = self.create_node(entry);
    let mut entries = self.entries.write().unwrap();
    entries.insert(path, (node, filetype));
  }

  fn create_node(&self, entry: FSEntry) -> u64 {
    let node = {
      let mut counter = self.node_counter.lock().unwrap();
      *counter += 1;
      *counter
    };
    let mut nodes = self.nodes.write().unwrap();
    nodes.insert(node, entry);
    node
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

  fn path_from_parts(&self, parent: &Path, name: &OsStr) -> PathBuf {
    let mut path = parent.to_path_buf();
    path.push(name);
    path
  }
}

impl FilesystemMT for FS {
  fn init(&self, _req:RequestInfo) -> ResultEmpty {
    Ok(())
  }

  fn opendir(&self, _req: RequestInfo, _path: &Path, _flags: u32) -> ResultOpen {
    Ok((0,0))
  }

  fn open(&self, _req: RequestInfo, path: &Path, flags: u32) -> ResultOpen {
    let node = match self.find_node(path) {
      Some(node) => node,
      None => return Err(libc::ENOENT),
    };
    let handle = self.create_handle(Handle{node: node.0, _flags: flags,});
    Ok((handle, flags))
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

  fn readdir(&self, _req: RequestInfo, path: &Path, _fh: u64) -> ResultReaddir {
    let mut dirlist = Vec::new();
    dirlist.push(DirectoryEntry{name: OsString::from("."), kind: FileType::Directory});
    dirlist.push(DirectoryEntry{name: OsString::from(".."), kind: FileType::Directory});
    for child in self.get_children(path) {
      let name = OsString::from(child.0.file_name().unwrap());
      let kind = child.1;
      dirlist.push(DirectoryEntry{name, kind,});    
    }
    Ok(dirlist)
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

  fn create(&self, _req: RequestInfo, parent: &Path, name: &OsStr, mode: u32, _flags: u32) -> ResultCreate {
    let path = self.path_from_parts(parent, name);
    let mut entry = FSEntry::new(FileType::RegularFile);
    entry.perm = mode;
    let created_entry = CreatedEntry {
      ttl: entry.ctime,
      attr: entry.attrs(),
      fh: 999,
      flags: entry.flags,
    };
    self.insert_entry(path, entry);
    Ok(created_entry)
  }

  fn mkdir(&self, _req: RequestInfo, parent: &Path, name: &OsStr, mode: u32) -> ResultEntry {
    let path = self.path_from_parts(parent, name);
    let mut entry = FSEntry::new(FileType::Directory);
    entry.perm = mode;
    let created_dir = (entry.ctime, entry.attrs());
    self.insert_entry(path, entry);
    Ok(created_dir)
  }

  fn symlink(&self, _req: RequestInfo, parent: &Path, name: &OsStr, target: &Path) -> ResultEntry {
    let path = self.path_from_parts(parent, name);
    let mut entry = FSEntry::new(FileType::Symlink);
    let data = target.as_os_str().as_bytes();
    let mut blockdata = [0; 4096];
    blockdata[0..data.len()].copy_from_slice(data);
    entry.blocks = vec![FSBlock{data: blockdata}];
    entry.perm = 0o777;
    entry.size = data.len() as u64;
    let created_symlink = (entry.ctime, entry.attrs());
    self.insert_entry(path, entry);
    Ok(created_symlink)
  }

  fn link(&self, _req: RequestInfo, path: &Path, newparent: &Path, newname: &OsStr) -> ResultEntry {
    let newpath = self.path_from_parts(newparent, newname);
    let node = match self.find_node(&path) {
      Some(node) => node,
      None => return Err(libc::ENOENT),
    };
    self.link_entry(newpath, node);
    self.with_node(node.0, &(|entry| (entry.ctime, entry.attrs())))
  }

  fn truncate(&self, _req: RequestInfo, path: &Path, fh: Option<u64>, size: u64) -> ResultEmpty {
    self.modify_path_optional_handle(path, fh, &(|entry| {
      entry.size = size;
    }))
  }

  fn write(&self, _req: RequestInfo, _path: &Path, fh: u64, offset: u64, data: Vec<u8>, _flags: u32) -> ResultWrite {
    let len = data.len() as u32;
    self.modify_handle(fh, &(|entry| {
      entry.write(offset, &data);
      len
    }))
  }

  fn read(&self, _req: RequestInfo, _path: &Path, fh: u64, offset: u64, size: u32) -> ResultData {
    self.with_handle(fh, &(|entry| entry.read(offset, size)))
  }

  fn readlink(&self, _req: RequestInfo, path: &Path) -> ResultData {
    self.with_path(path, &(|entry| entry.blocks[0].data[0..entry.size as usize].to_vec()))
  }

  fn rmdir(&self, _req: RequestInfo, parent: &Path, name: &OsStr) -> ResultEmpty {
    let path = self.path_from_parts(parent, name);
    if self.get_children(&path).len() > 0 {
      return Err(libc::ENOTEMPTY)
    }
    self.remove_entry(&path);
    Ok(())
  }

  fn unlink(&self, _req: RequestInfo, parent: &Path, name: &OsStr) -> ResultEmpty {
    let path = self.path_from_parts(parent, name);
    self.remove_entry(&path);
    Ok(())
  }

  fn rename(&self, _req: RequestInfo, parent: &Path, name: &OsStr, newparent: &Path, newname: &OsStr) -> ResultEmpty {
    let path = self.path_from_parts(parent, name);
    let newpath = self.path_from_parts(newparent, newname);
    match self.remove_entry(&path) {
      Some(node) => self.link_entry(newpath, node),
      None => return Err(libc::ENOENT),
    };
    Ok(())
  }
}

fn main() {
  let fs = FS::new();
  fs.insert_entry(PathBuf::from("/"), FSEntry::new(FileType::Directory));
  fs.insert_entry(PathBuf::from("/foo"), FSEntry::new(FileType::Directory));
  fs.insert_entry(PathBuf::from("/foo/bar"), FSEntry::new(FileType::RegularFile));
  fs.insert_entry(PathBuf::from("/foo/baz"), FSEntry::new(FileType::RegularFile));
  fs.insert_entry(PathBuf::from("/foo2"), FSEntry::new(FileType::Directory));

  let fs_mt = FuseMT::new(fs, 16);
  let path = "mnt".to_string();
  let options = [OsStr::new("-o"), OsStr::new("auto_unmount")];
  println!("Starting filesystem in {:?}", path);
  match fuse_mt::mount(fs_mt, &path, &options[..]) {
    Ok(_) => {},
    Err(e) => eprintln!("FUSE error: {:?}", e),
  };
}
