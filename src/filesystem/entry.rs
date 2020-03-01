extern crate fuse_mt;
use self::fuse_mt::*;
extern crate libc;
use self::libc::c_int;
extern crate time;
use self::time::Timespec;

use std::ffi::{OsStr, OsString};
use std::cmp;
// Not using HashMap because of https://github.com/TyOverby/bincode/issues/230
use std::collections::BTreeMap;

use super::vclock::*;
use crate::backingstore::*;
use crate::settings::*;

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

macro_rules! merge_3way {
  ($base:expr, $left:expr, $right:expr) => {
    if $left == $base {
      $right.clone()
    } else {
      $left.clone()
    }
  }
}

macro_rules! merge_3way_hash {
  ($base:expr, $left:expr, $right:expr) => {
    {
      let mut keys: Vec<&String> = $left.keys().collect();
      let mut otherkeys: Vec<&String> = $right.keys().collect();
      keys.append(&mut otherkeys);

      let mut newkeys = BTreeMap::new();
      for k in keys {
        let b = $base.get(k);
        let l = $left.get(k);
        let r = $right.get(k);
        let m = merge_3way!(b, l, r);
        if let Some(m) = m {
          newkeys.insert(k.clone(), m.clone());
        }
      }
      newkeys
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
  pub children: BTreeMap<String, (NodeId, FileTypeDef)>,
  pub xattrs: BTreeMap<String, Vec<u8>>,
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
      children: BTreeMap::new(),
      xattrs: BTreeMap::new(),
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
    self.children.insert(from_os_str(name)?, node);
    Ok(())
  }

  pub fn remove_child(&mut self, name: &OsStr) -> Result<(NodeId, FileTypeDef), c_int> {
    match self.children.remove(&from_os_str(name)?) {
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
      bs.write(node, i, block, boffset, &data[written..written+bsize], readahead)?;
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
      data[written..written+bsize].copy_from_slice(&bs.read(node, i, block, boffset, bsize, readahead)?);
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

  pub fn timeval(&self) -> i64 {
    self.clock.sec * 1000 + (self.clock.nsec as i64)/1000000
  }

  pub fn merge_3way(&self, first: &FSEntry, second: &FSEntry) -> FSEntry {
    assert!(first.filetype == second.filetype);

    let first_large = first.clock > second.clock || first.peernum > second.peernum;
    let (left, right) = if first_large { (first, second) } else { (second, first) };

    FSEntry {
      clock: cmp::max(left.clock, right.clock),
      vclock: left.vclock.merge(&right.vclock),
      peernum: cmp::max(left.peernum, right.peernum),
      filetype: left.filetype,
      perm: merge_3way!(self.perm, left.perm, right.perm),
      uid: merge_3way!(self.uid, left.uid, right.uid),
      gid: merge_3way!(self.gid, left.gid, right.gid),
      flags: merge_3way!(self.flags, left.flags, right.flags),
      rdev: merge_3way!(self.rdev, left.rdev, right.rdev),
      atime: cmp::max(left.atime, right.atime),
      mtime: cmp::max(left.mtime, right.mtime),
      ctime: cmp::max(left.ctime, right.ctime),
      crtime: cmp::max(left.crtime, right.crtime),
      chgtime: cmp::max(left.chgtime, right.chgtime),
      bkuptime: cmp::max(left.bkuptime, right.bkuptime),
      size: merge_3way!(self.size, left.size, right.size),
      blocks: merge_3way!(self.blocks, left.blocks, right.blocks),
      children: merge_3way_hash!(self.children, left.children, right.children),
      xattrs: merge_3way_hash!(self.xattrs, left.xattrs, right.xattrs),
    }
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
    let encoded2: Vec<u8> = bincode::serialize(&entry2).unwrap();

    assert_eq!(entry, entry2);
    assert_eq!(encoded, encoded2);
  }

  #[test]
  fn three_way_merge() {
    let base   = FSEntry::new(FileTypeDef::RegularFile, 0);
    let mut first  = FSEntry::new(FileTypeDef::RegularFile, 0);
    let mut second = FSEntry::new(FileTypeDef::RegularFile, 0);

    first.peernum = 1;
    first.perm = 10;
    first.vclock.increment(1);
    second.peernum = 2;
    second.blocks = vec![[0;HASHSIZE]];
    second.vclock.increment(2);
    second.children.insert("test".to_string(), ((0,0), FileTypeDef::RegularFile));

    let merge1 = base.merge_3way(&first, &second);
    let merge2 = base.merge_3way(&second, &first);

    assert_eq!(merge1, merge2);
    assert_eq!(first.perm, merge1.perm);
    assert_eq!(second.blocks, merge1.blocks);
    assert_eq!(2, merge1.peernum);
    assert_eq!(second.children, merge1.children);

    let mut newvclock = VectorClock::new();
    newvclock.increment(1);
    newvclock.increment(2);

    assert_eq!(newvclock, merge1.vclock);
  }

  #[test]
  fn children_merge() {
    let base   = FSEntry::new(FileTypeDef::RegularFile, 0);
    let mut first  = FSEntry::new(FileTypeDef::RegularFile, 0);
    let mut second = FSEntry::new(FileTypeDef::RegularFile, 0);

    first.children.insert("foo".to_string(), ((1,1), FileTypeDef::RegularFile));
    second.children.insert("bar".to_string(), ((2,2), FileTypeDef::RegularFile));

    let merge1 = base.merge_3way(&first, &second);
    let merge2 = base.merge_3way(&second, &first);
    assert_eq!(merge1, merge2);

    let mut result = base.clone();
    result.children.insert("foo".to_string(), ((1,1), FileTypeDef::RegularFile));
    result.children.insert("bar".to_string(), ((2,2), FileTypeDef::RegularFile));

    assert_eq!(result.children, merge1.children);
  }

  #[test]
  fn children_remove() {
    let mut base   = FSEntry::new(FileTypeDef::RegularFile, 0);
    base.children.insert("foo".to_string(), ((1,1), FileTypeDef::RegularFile));
    base.children.insert("bar".to_string(), ((2,2), FileTypeDef::RegularFile));

    let mut first  = FSEntry::new(FileTypeDef::RegularFile, 0);
    first.children.insert("foo".to_string(), ((1,1), FileTypeDef::RegularFile));
    first.children.insert("bar".to_string(), ((2,2), FileTypeDef::RegularFile));

    let mut second = FSEntry::new(FileTypeDef::RegularFile, 0);
    second.children.insert("foo".to_string(), ((1,1), FileTypeDef::RegularFile));

    let merge1 = base.merge_3way(&first, &second);
    let merge2 = base.merge_3way(&second, &first);
    assert_eq!(merge1, merge2);

    assert_eq!(second.children, merge1.children);
  }

  #[test]
  fn xattrs_merge() {
    let base   = FSEntry::new(FileTypeDef::RegularFile, 0);
    let mut first  = FSEntry::new(FileTypeDef::RegularFile, 0);
    let mut second = FSEntry::new(FileTypeDef::RegularFile, 0);

    first.xattrs.insert("foo".to_string(), vec![0]);
    second.xattrs.insert("bar".to_string(), vec![0]);

    let merge1 = base.merge_3way(&first, &second);
    let merge2 = base.merge_3way(&second, &first);
    assert_eq!(merge1, merge2);

    let mut result = base.clone();
    result.xattrs.insert("foo".to_string(), vec![0]);
    result.xattrs.insert("bar".to_string(), vec![0]);

    assert_eq!(result.xattrs, merge1.xattrs);
  }

  #[test]
  fn xattrs_remove() {
    let mut base   = FSEntry::new(FileTypeDef::RegularFile, 0);
    base.xattrs.insert("foo".to_string(), vec![0]);
    base.xattrs.insert("bar".to_string(), vec![0]);

    let mut first  = FSEntry::new(FileTypeDef::RegularFile, 0);
    first.xattrs.insert("foo".to_string(), vec![0]);
    first.xattrs.insert("bar".to_string(), vec![0]);

    let mut second = FSEntry::new(FileTypeDef::RegularFile, 0);
    second.xattrs.insert("foo".to_string(), vec![0]);

    let merge1 = base.merge_3way(&first, &second);
    let merge2 = base.merge_3way(&second, &first);
    assert_eq!(merge1, merge2);

    assert_eq!(second.xattrs, merge1.xattrs);
  }
}
