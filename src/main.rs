extern crate time;
extern crate fuse_mt;
extern crate libc;

use fuse_mt::*;
use std::path::{Path,PathBuf};
use std::ffi::{OsStr, OsString};
use std::collections::BTreeMap;
use std::sync::Mutex;
use time::Timespec;
use libc::{ENOENT,ENOTEMPTY, c_int};
use std::cmp;

#[derive(Debug, Clone)]
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
  data: Vec<u8>,
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
      data: Vec::new(),
    }
  }

  fn attrs(&self) -> FileAttr {
    FileAttr {
      size: self.data.len() as u64,
      blocks: 0,
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
}

#[derive(Debug)]
struct FS {
  entries: Mutex<BTreeMap<PathBuf, FSEntry>>,
}

impl FS {
  fn new() -> FS {
    let mut entries = BTreeMap::new();
    entries.insert(PathBuf::from("/"), FSEntry::new(FileType::Directory));
    entries.insert(PathBuf::from("/foo"), FSEntry::new(FileType::Directory));
    entries.insert(PathBuf::from("/foo/bar"), FSEntry::new(FileType::RegularFile));
    entries.insert(PathBuf::from("/foo/baz"), FSEntry::new(FileType::RegularFile));
    entries.insert(PathBuf::from("/foo2"), FSEntry::new(FileType::Directory));

    FS {
      entries: Mutex::new(entries),
    }
  }

  fn get_entry(&self, path: &Path) -> Result<FSEntry, c_int> {
    let entries = self.entries.lock().unwrap();
    match entries.get(&(path.to_path_buf())) {
      Some(e) => Ok(e.clone()),
      None => Err(ENOENT),
    }
  }

  fn get_children(&self, path: &Path) -> Vec<(PathBuf, FSEntry)> {
    // List all the children of a given path by iterating the BTreeMap
    // This isn't particularly efficient for directories with a lot of children
    // directories because the sorting order results in a depth first search. Fixing
    // the sorting order would probably fix that.

    let mut children = Vec::new();
    let entries = self.entries.lock().unwrap();

    for child in entries.range(path.to_path_buf()..) {
      // It's the path itself, skip
      if child.0 == path { continue }
      // This is not a child of the dir
      if child.0.parent().unwrap() != path { continue }
      // We're past the dir itself
      if !child.0.starts_with(path) { break }

      children.push((child.0.clone(), child.1.clone()));
    }

    children
  }

  fn modify_entry<F>(&self, path: &Path, closure: &F) -> ResultEmpty
    where F : Fn(&mut FSEntry) {
    let mut entry = try!(self.get_entry(path));
    closure(&mut entry);
    self.insert_entry(path.to_path_buf(), entry);
    Ok(())
  }

  fn remove_entry(&self, path: &Path) {
    let mut entries = self.entries.lock().unwrap();
    entries.remove(path);
  }

  fn insert_entry(&self, path: PathBuf, entry: FSEntry) {
    let mut entries = self.entries.lock().unwrap();
    entries.insert(path, entry);
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

  fn getattr(&self, _req: RequestInfo, path: &Path, _fh: Option<u64>) -> ResultEntry {
    let entry = try!(self.get_entry(path));
    let time = time::get_time();
    Ok((time, entry.attrs()))
  }

  fn readdir(&self, _req: RequestInfo, path: &Path, _fh: u64) -> ResultReaddir {
    let mut dirlist = Vec::new();
    dirlist.push(DirectoryEntry{name: OsString::from("."), kind: FileType::Directory});
    dirlist.push(DirectoryEntry{name: OsString::from(".."), kind: FileType::Directory});
    for child in self.get_children(path) {
      let name = OsString::from(child.0.file_name().unwrap());
      let kind = child.1.filetype;
      dirlist.push(DirectoryEntry{name, kind,});    
    }
    Ok(dirlist)
  }

  fn chmod(&self, _req: RequestInfo, path: &Path, _fh: Option<u64>, mode: u32) -> ResultEmpty {
    self.modify_entry(path, &(|entry| {
      entry.perm = mode;
    }))
  }

  fn chown(&self, _req: RequestInfo, path: &Path, _fh: Option<u64>, uid: Option<u32>, gid: Option<u32>) -> ResultEmpty {
    self.modify_entry(path, &(|entry| {
      if let Some(uid) = uid {entry.uid = uid};
      if let Some(gid) = gid {entry.gid = gid};
    }))
  }

  fn utimens(&self, _req: RequestInfo, path: &Path, _fh: Option<u64>, atime: Option<Timespec>, mtime: Option<Timespec>) -> ResultEmpty {
    self.modify_entry(path, &(|entry| {
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

  fn truncate(&self, _req: RequestInfo, path: &Path, _fh: Option<u64>, size: u64) -> ResultEmpty {
    self.modify_entry(path, &(|entry| {
      let size = size as usize;
      entry.data.resize(size, 0);
    }))
  }

  fn write(&self, _req: RequestInfo, path: &Path, _fh: u64, offset: u64, data: Vec<u8>, _flags: u32) -> ResultWrite {
    let mut entry = try!(self.get_entry(path));
    let len = data.len() as u32;
    let total_needed_size = (offset as usize) + data.len();
    if total_needed_size > entry.data.len() {
      entry.data.resize(total_needed_size, 0);
    }
    let off = offset as usize;
    entry.data[off..off + data.len()].copy_from_slice(&data[..]);
    self.insert_entry(path.to_path_buf(), entry);
    Ok(len)
  }

  fn read(&self, _req: RequestInfo, path: &Path, _fh: u64, offset: u64, size: u32) -> ResultData {
    let entry = try!(self.get_entry(path));
    let start = offset as usize;
    let end = cmp::min(start + (size as usize), entry.data.len());
    Ok(entry.data[start..end].to_vec())
  }

  fn rmdir(&self, _req: RequestInfo, parent: &Path, name: &OsStr) -> ResultEmpty {
    let path = self.path_from_parts(parent, name);
    if self.get_children(&path).len() > 0 {
      return Err(ENOTEMPTY)
    }
    self.remove_entry(&path);
    Ok(())
  }

  fn unlink(&self, _req: RequestInfo, parent: &Path, name: &OsStr) -> ResultEmpty {
    let path = self.path_from_parts(parent, name);
    self.remove_entry(&path);
    Ok(())
  }
}

fn main() {
  let fs = FS::new();
  let fs_mt = FuseMT::new(fs, 16);
  let path = "mnt".to_string();
  let options = [OsStr::new("-o"), OsStr::new("auto_unmount")];
  println!("Starting filesystem in {:?}", path);
  match fuse_mt::mount(fs_mt, &path, &options[..]) {
    Ok(_) => {},
    Err(e) => eprintln!("FUSE error: {:?}", e),
  };
}
