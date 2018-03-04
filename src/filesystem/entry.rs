extern crate fuse_mt;
use self::fuse_mt::*;
extern crate libc;
use self::libc::c_int;
extern crate time;
use self::time::Timespec;

use std::ffi::{OsStr, OsString};
use std::collections::HashMap;
use std::cmp;

use super::vclock::*;
use backingstore::*;
use settings::*;

#[derive(Serialize, Deserialize)]
#[serde(remote = "Timespec")]
#[allow(dead_code)]
struct TimespecDef {
  sec: i64,
  nsec: i32,
}

#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum FileTypeDef {
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FSEntry {
  #[serde(with = "TimespecDef")]
  pub clock: Timespec,
  pub vclock: VectorClock,
  pub peernum: i64,

  pub filetype: FileTypeDef,
  pub perm: u32,
  pub uid: u32,
  pub gid: u32,
  pub flags: u32,
  pub rdev: u32,
  #[serde(with = "TimespecDef")]
  pub atime: Timespec,
  #[serde(with = "TimespecDef")]
  pub mtime: Timespec,
  #[serde(with = "TimespecDef")]
  pub ctime: Timespec,
  #[serde(with = "TimespecDef")]
  pub crtime: Timespec,
  #[serde(with = "TimespecDef")]
  pub chgtime: Timespec,
  #[serde(with = "TimespecDef")]
  pub bkuptime: Timespec,
  pub size: u64,
  pub blocks: Vec<BlobHash>,
  pub children: HashMap<String, (NodeId, FileTypeDef)>,
  pub xattrs: HashMap<String, Vec<u8>>,
}

pub fn from_os_str(ostr: &OsStr) -> Result<String, c_int> {
  ostr.to_os_string().into_string().or_else(|_| Err(libc::EIO))
}

impl FSEntry {
  pub fn new(filetype: FileTypeDef, peernum: i64) -> FSEntry {
    let time = self::time::get_time();

    FSEntry {
      clock: time,
      vclock: VectorClock::new(),
      peernum: peernum,
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

  pub fn attrs(&self) -> FileAttr {
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

  pub fn children(&self) -> Vec<DirectoryEntry> {
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

  pub fn add_child(&mut self, name: &OsStr, node: (NodeId, FileTypeDef)) -> Result<(), c_int> {
    self.children.insert(try!(from_os_str(name)), node);
    Ok(())
  }

  pub fn remove_child(&mut self, name: &OsStr) -> Result<(NodeId, FileTypeDef), c_int> {
    match self.children.remove(&try!(from_os_str(name))) {
      None => Err(libc::ENOENT),
      Some(c) => Ok(c),
    }
  }

  pub fn write(&mut self, node: NodeId, bs: &BackingStore, offset: u64, data: &[u8]) -> Result<u32, c_int> {
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
      let readahead = &self.blocks[i+1..cmp::min(i+1+READAHEAD, self.blocks.len())];
      let bstart = cmp::max(start, i*BLKSIZE);
      let bend = cmp::min(end, (i+1)*BLKSIZE);
      let bsize = bend - bstart;
      let boffset = bstart - i*BLKSIZE;
      try!(bs.write(node, i, block, boffset, &data[written..written+bsize], readahead));
      written += bsize;
    }
    assert!(written == data.len());
    self.mtime = self::time::get_time();
    Ok(written as u32)
  }

  pub fn read(&self, node: NodeId, bs: &BackingStore, offset: u64, size: u32) -> Result<Vec<u8>, c_int> {
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
      let readahead = &self.blocks[i+1..cmp::min(i+1+READAHEAD, self.blocks.len())];
      let bstart = cmp::max(start, i*BLKSIZE);
      let bend = cmp::min(end, (i+1)*BLKSIZE);
      let bsize = bend - bstart;
      let boffset = bstart - i*BLKSIZE;
      data[written..written+bsize].copy_from_slice(&try!(bs.read(node, i, block, boffset, bsize, readahead)));
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

  pub fn cmp_vclock(&self, other: &Self) -> VectorOrdering {
    self.vclock.cmp(&other.vclock)
  }

  pub fn cmp_time(&self, other: &Self) -> cmp::Ordering {
    match self.clock.cmp(&other.clock) {
      cmp::Ordering::Equal => self.peernum.cmp(&other.peernum),
      o => o,
    }
  }

  pub fn timeval(&self) -> i64 {
    self.clock.sec * 1000 + (self.clock.nsec as i64)/1000000
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  extern crate bincode;

  #[test]
  fn serialization_roundtrips() {
    let mut entry = FSEntry::new(FileTypeDef::Directory, 0);
    entry.vclock.increment(0);
    entry.vclock.increment(1);
    let encoded: Vec<u8> = bincode::serialize(&entry).unwrap();
    let entry2: FSEntry = bincode::deserialize(&encoded).unwrap();
    //let encoded2: Vec<u8> = bincode::serialize(&entry2).unwrap();

    assert_eq!(entry, entry2);
    // This won't work because of https://github.com/TyOverby/bincode/issues/230
    //assert_eq!(encoded, encoded2);
  }
}
