extern crate rusqlite;
extern crate libc;
extern crate hex;
extern crate time;

use super::blobstorage::*;
use self::rusqlite::Connection;
use self::libc::c_int;
use std::sync::Mutex;

pub fn timeval() -> i64 {
  let time = time::get_time();
  time.sec * 1000 + (time.nsec as i64)/1000000
}

pub struct MetadataDB {
  connection: Mutex<Connection>,
}

fn dberror_print(error: self::rusqlite::Error) {
  eprintln!("WARNING: db error: \"{}\"", error);
}

macro_rules! dberror_test {
  ( $e:expr ) => {
    match $e {
      Ok(_) => {},
      Err(e) => dberror_print(e),
    }
  }
}

macro_rules! dberror_return {
  ( $e:expr ) => {
    match $e {
      Ok(vals) => vals,
      Err(e) => {dberror_print(e); return Err(libc::EIO)},
    }
  }
}


impl MetadataDB {
  fn hash_from_string(hash: String) -> BlobHash {
    assert!(hash.len() == HASHSIZE*2);
    let mut hasharray = [0; HASHSIZE];
    let vals = hex::decode(hash).unwrap();
    for i in 0..HASHSIZE {
      hasharray[i] = vals[i];
    }
    hasharray
  }

  pub fn new(connection: Connection) -> Self {
    // Make the database faster at the cost of losing data but without causing corruption
    // https://www.sqlite.org/pragma.html#pragma_synchronous
    // If durability is not a concern, then synchronous=NORMAL is normally all one needs
    // in WAL mode.
    connection.execute("PRAGMA journal_mode=WAL", &[]).ok();
    connection.execute("PRAGMA synchronous=NORMAL", &[]).ok();

    connection.execute("CREATE TABLE IF NOT EXISTS nodes (
      id              INTEGER NOT NULL,
      hash            TEXT NOT NULL,
      creation        INTEGER NOT NULL
    )", &[]).unwrap();

    connection.execute("CREATE TABLE IF NOT EXISTS blobs (
      hash            TEXT PRIMARY KEY,
      synced          INTEGER NOT NULL,
      present         INTEGER NOT NULL,
      size            INTEGER NOT NULL,
      last_use        INTEGER NOT NULL
    )", &[]).unwrap();

    Self {
      connection: Mutex::new(connection),
    }
  }

  pub fn max_node(&self) -> Result<u64, c_int> {
    let conn = self.connection.lock().unwrap();
    let node: i64 = dberror_return!(conn.query_row(
      "SELECT COALESCE(MAX(id), 0) FROM nodes",
      &[], |row| row.get(0)));
    Ok(node as u64)
  }

  pub fn get_node(&self, node: u64) -> Result<BlobHash, c_int> {
    let conn = self.connection.lock().unwrap();
    let hash: String = dberror_return!(conn.query_row(
      "SELECT hash FROM nodes WHERE id=?1 ORDER BY creation DESC LIMIT 1",
      &[&(node as i64)], |row| row.get(0)));
    Ok(Self::hash_from_string(hash))
  }

  pub fn set_node(&self, node: u64, hash: &BlobHash) -> Result<(), c_int> {
    let time = timeval();
    let conn = self.connection.lock().unwrap();
    dberror_return!(conn.execute(
      "INSERT INTO nodes (id, hash, creation) VALUES (?1, ?2, ?3)",
      &[&(node as i64), &(hex::encode(hash)), &time]));
    Ok(())
  }

  #[allow(dead_code)] pub fn get_blob(&self, hash: &BlobHash) -> Result<(bool, u64, i64), c_int> {
    let conn = self.connection.lock().unwrap();
    let vals: (i64, i64, i64) = dberror_return!(conn.query_row(
      "SELECT synced, size, last_use FROM blobs WHERE hash=?1",
      &[&(hex::encode(hash))], |row| (row.get(0), row.get(1), row.get(2))));
    Ok((vals.0 != 0, vals.1 as u64, vals.2))
  }

  #[allow(dead_code)] pub fn set_blob(&self, hash: &BlobHash, size: u64) {
    let mut vals = vec![(hash.clone(), size, timeval())];
    self.set_blobs(vals.drain(..));
  }

  pub fn set_blobs<I>(&self, vals: I)
    where I: Iterator<Item = (BlobHash, u64, i64)> {
    let mut conn = self.connection.lock().unwrap();
    let tran = conn.transaction().unwrap();
    for (hash, size, time) in vals {
      dberror_test!(tran.execute(
        "INSERT OR REPLACE INTO blobs (hash, size, last_use, present, synced)
         VALUES (?1, ?2, ?3, 1,COALESCE((SELECT synced FROM blobs WHERE hash = ?1), 0))",
        &[&(hex::encode(hash)), &(size as i64), &time]));
    }
    tran.commit().unwrap();
  }

  pub fn touch_blobs<I>(&self, vals: I)
    where I: Iterator<Item = (BlobHash, i64)> {
    let mut conn = self.connection.lock().unwrap();
    let tran = conn.transaction().unwrap();
    for (hash, time) in vals {
      dberror_test!(tran.execute(
        "UPDATE OR IGNORE blobs SET last_use = ?2 WHERE hash = ?1",
         &[&(hex::encode(hash)), &time]));
    }
    tran.commit().unwrap();
  }

  #[allow(dead_code)] pub fn mark_synced_blob(&self, hash: &BlobHash) {
    let mut vals = vec![hash.clone()];
    self.mark_synced_blobs(vals.drain(..));
  }

  pub fn mark_synced_blobs<I>(&self, vals: I)
    where I: Iterator<Item = BlobHash> {
    let mut conn = self.connection.lock().unwrap();
    let tran = conn.transaction().unwrap();
    for hash in vals {
      dberror_test!(tran.execute(
        "UPDATE OR IGNORE blobs SET synced = 1 WHERE hash = ?1",
        &[&(hex::encode(hash))]));
    }
    tran.commit().unwrap();
  }

  pub fn mark_deleted_blobs(&self, vals: &[BlobHash], deleted: bool) {
    let mut conn = self.connection.lock().unwrap();
    let tran = conn.transaction().unwrap();
    let present: i64 = if deleted { 0 } else { 1 };
    for hash in vals {
      dberror_test!(
        tran.execute("UPDATE OR IGNORE blobs SET present = ?2 WHERE hash = ?1",
        &[&(hex::encode(hash)), &present]));
    }
    tran.commit().unwrap();
  }

  pub fn to_upload(&self) -> Vec<BlobHash> {
    let conn = self.connection.lock().unwrap();
    let mut stmt = conn.prepare(
      "SELECT hash FROM blobs WHERE synced = 0 ORDER BY last_use ASC").unwrap();
    let hash_iter = stmt.query_map(&[], |row| {
      Self::hash_from_string(row.get(0))
    }).unwrap();
    let mut hashes = Vec::new();
    for hash in hash_iter {
      hashes.push(hash.unwrap());
    }
    hashes
  }

  pub fn to_delete(&self) -> Vec<(BlobHash, u64)> {
    let conn = self.connection.lock().unwrap();
    let mut stmt = conn.prepare(
      "SELECT hash, size FROM blobs WHERE synced = 1 and present = 1
       ORDER BY last_use ASC LIMIT 1000").unwrap();
    let hash_iter = stmt.query_map(&[], |row| {
      let hasharray = Self::hash_from_string(row.get(0));
      let size: i64 = row.get(1);
      (hasharray, size as u64)
    }).unwrap();
    let mut vec = Vec::new();
    for hash in hash_iter {
      vec.push(hash.unwrap());
    }
    vec
  }

  pub fn localbytes(&self) -> u64 {
    let conn = self.connection.lock().unwrap();
    let bytes: i64 = conn.query_row(
      "SELECT COALESCE(SUM(size), 0) FROM blobs WHERE present=1",
      &[], |row| row.get(0)).unwrap();
    bytes as u64
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std;

  #[test]
  fn set_and_get_node() {
    let conn = Connection::open_in_memory().unwrap();
    let db = MetadataDB::new(conn);
    let from_hash = [0;HASHSIZE];
    db.set_node(0, &from_hash).unwrap();
    let hash = db.get_node(0).unwrap();
    assert_eq!(from_hash, hash);
  }

  #[test]
  fn maxnode() {
    let conn = Connection::open_in_memory().unwrap();
    let db = MetadataDB::new(conn);
    assert_eq!(0, db.max_node().unwrap());
    let from_hash = [0;HASHSIZE];
    db.set_node(5, &from_hash).unwrap();
    assert_eq!(5, db.max_node().unwrap());
  }

  #[test]
  fn set_and_get_blob() {
    let conn = Connection::open_in_memory().unwrap();
    let db = MetadataDB::new(conn);
    let from_hash = [0;HASHSIZE];
    let from_size = 10;
    db.set_blob(&from_hash, from_size);
    let from_time = timeval();
    let (synced, size, last_used) = db.get_blob(&from_hash).unwrap();
    assert_eq!(from_size, size);
    assert_eq!(false, synced);
    assert!(from_time >= last_used);
    std::thread::sleep(std::time::Duration::from_millis(10));
    let mut vals = vec![(from_hash, timeval())];
    db.touch_blobs(vals.drain(..));
    db.mark_synced_blob(&from_hash);
    let (synced, _, new_last_used) = db.get_blob(&from_hash).unwrap();
    assert!(new_last_used > last_used);
    assert_eq!(true, synced);
  }

  #[test]
  fn set_and_reset_blob() {
    let conn = Connection::open_in_memory().unwrap();
    let db = MetadataDB::new(conn);
    let from_hash = [0;HASHSIZE];
    db.set_blob(&from_hash, 0);
    db.mark_synced_blob(&from_hash);
    let (_, _, last_used) = db.get_blob(&from_hash).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(10));
    db.set_blob(&from_hash, 0);
    let (synced, _, new_last_used) = db.get_blob(&from_hash).unwrap();
    assert!(new_last_used > last_used);
    assert_eq!(true, synced);
  }

  #[test]
  fn to_upload() {
    let conn = Connection::open_in_memory().unwrap();
    let db = MetadataDB::new(conn);
    let from_hash1 = [1;HASHSIZE];
    let from_hash2 = [2;HASHSIZE];
    let from_hash3 = [3;HASHSIZE];
    db.set_blob(&from_hash1, 0);
    db.set_blob(&from_hash2, 0);
    db.set_blob(&from_hash3, 0);
    db.mark_synced_blob(&from_hash2);
    let to_upload: Vec<BlobHash> = db.to_upload().into();
    assert_eq!(vec![from_hash1, from_hash3], to_upload);
  }

  #[test]
  fn to_delete() {
    let conn = Connection::open_in_memory().unwrap();
    let db = MetadataDB::new(conn);
    let from_hash1 = [1;HASHSIZE];
    let from_hash2 = [2;HASHSIZE];
    let from_hash3 = [3;HASHSIZE];
    let from_hash4 = [4;HASHSIZE];
    db.set_blob(&from_hash1, 10);
    db.set_blob(&from_hash2, 20);
    db.set_blob(&from_hash3, 30);
    db.set_blob(&from_hash4, 40);
    db.mark_synced_blob(&from_hash2);
    db.mark_synced_blob(&from_hash3);
    db.mark_synced_blob(&from_hash4);
    assert_eq!(vec![(from_hash2, 20), (from_hash3, 30), (from_hash4, 40)], db.to_delete());
    db.mark_deleted_blobs(&[from_hash2], true);
    assert_eq!(vec![(from_hash3, 30), (from_hash4, 40)], db.to_delete());
    db.mark_deleted_blobs(&[from_hash2], false);
    assert_eq!(vec![(from_hash2, 20), (from_hash3, 30), (from_hash4, 40)], db.to_delete());
  }

  #[test]
  fn localbytes() {
    let conn = Connection::open_in_memory().unwrap();
    let db = MetadataDB::new(conn);
    assert_eq!(0, db.localbytes());
    let from_hash1 = [1;HASHSIZE];
    let from_hash2 = [2;HASHSIZE];
    db.set_blob(&from_hash1, 10);
    db.set_blob(&from_hash2, 20);
    assert_eq!(30, db.localbytes());
    db.mark_deleted_blobs(&[from_hash2], true);
    assert_eq!(10, db.localbytes());
    db.mark_deleted_blobs(&[from_hash2], false);
    assert_eq!(30, db.localbytes());
  }
}
