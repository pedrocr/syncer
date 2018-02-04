extern crate rusqlite;
extern crate libc;
extern crate hex;
extern crate time;

use super::blobstorage::*;
use self::rusqlite::Connection;
use self::libc::c_int;
use std::sync::Mutex;
use std::collections::VecDeque;

fn timeval() -> i64 {
  let time = time::get_time();
  time.sec * 1000 + (time.nsec as i64)/1000000
}

pub struct MetadataDB {
  connection: Mutex<Connection>,
}

impl MetadataDB {
  pub fn new(connection: Connection) -> Self {
    connection.execute("CREATE TABLE IF NOT EXISTS nodes (
      id              INTEGER PRIMARY KEY,
      hash            TEXT NOT NULL
    )", &[]).unwrap();

    connection.execute("CREATE TABLE IF NOT EXISTS blobs (
      hash            TEXT PRIMARY KEY,
      synced          INTEGER NOT NULL,
      size            INTEGER NOT NULL,
      last_use        INTEGER NOT NULL
    )", &[]).unwrap();

    Self {
      connection: Mutex::new(connection),
    }
  }

  pub fn get_node(&self, node: u64) -> Result<BlobHash, c_int> {
    let conn = self.connection.lock().unwrap();
    let hash: String = match conn.query_row("SELECT hash FROM nodes WHERE id=?1",
                                             &[&(node as i64)], |row| row.get(0)) {
      Ok(hash) => hash,
      Err(_) => return Err(libc::EIO),
    };
    assert!(hash.len() == HASHSIZE*2);
    let mut hasharray = [0; HASHSIZE];
    let vals = hex::decode(hash).unwrap();
    for i in 0..HASHSIZE {
      hasharray[i] = vals[i];
    }
    Ok(hasharray)
  }

  pub fn set_node(&self, node: u64, hash: &BlobHash) -> Result<(), c_int> {
    let conn = self.connection.lock().unwrap();
    match conn.execute("INSERT OR REPLACE INTO nodes (id, hash) VALUES (?1, ?2)",
                 &[&(node as i64), &(hex::encode(hash))]) {
      Ok(_) => Ok(()),
      Err(_) => return Err(libc::EIO),
    }
  }

  pub fn get_blob(&self, hash: &BlobHash) -> Result<(bool, u64, i64), c_int> {
    let conn = self.connection.lock().unwrap();
    let vals: (i64, i64, i64) = match conn.query_row(
      "SELECT synced, size, last_use FROM blobs WHERE hash=?1",
      &[&(hex::encode(hash))], |row| (row.get(0), row.get(1), row.get(2))) {
      Ok(vals) => vals,
      Err(e) => {println!("error is {:?}", e); return Err(libc::EIO)},
    };
    Ok((vals.0 != 0, vals.1 as u64, vals.2))
  }

  pub fn set_blob(&self, hash: &BlobHash, size: u64) -> Result<(), c_int> {
    let conn = self.connection.lock().unwrap();
    let time = timeval();
    match conn.execute(
      "INSERT OR REPLACE INTO blobs (hash, size, last_use, synced) VALUES (?1, ?2, ?3,
         COALESCE((SELECT synced FROM blobs WHERE hash = ?1), 0))",
      &[&(hex::encode(hash)), &(size as i64), &time]) {
      Ok(_) => Ok(()),
      Err(e) => {println!("error is {:?}", e); return Err(libc::EIO)},
    }
  }

  pub fn touch_blob(&self, hash: &BlobHash) {
    let conn = self.connection.lock().unwrap();
    let time = timeval();
    match conn.execute("UPDATE OR IGNORE blobs SET last_use = ?2 WHERE hash = ?1",
                 &[&(hex::encode(hash)), &time]) {
      Ok(_) => {},
      Err(e) => {println!("error is {:?}", e);},
    };
  }

  pub fn mark_synced_blob(&self, hash: &BlobHash) {
    let conn = self.connection.lock().unwrap();
    match conn.execute("UPDATE OR IGNORE blobs SET synced = 1 WHERE hash = ?1",
                 &[&(hex::encode(hash))]) {
      Ok(_) => {},
      Err(e) => {println!("error is {:?}", e);},
    };
  }

  pub fn to_upload(&self) -> VecDeque<BlobHash> {
    let conn = self.connection.lock().unwrap();
    let mut stmt = conn.prepare("SELECT hash FROM blobs WHERE synced = 0 ORDER BY last_use ASC").unwrap();
    let hash_iter = stmt.query_map(&[], |row| {
      let hash: String = row.get(0);
      assert!(hash.len() == HASHSIZE*2);
      let mut hasharray = [0; HASHSIZE];
      let vals = hex::decode(hash).unwrap();
      for i in 0..HASHSIZE {
        hasharray[i] = vals[i];
      }
      hasharray
    }).unwrap();
    let mut deq = VecDeque::new();
    for hash in hash_iter {
      deq.push_back(hash.unwrap());
    }
    deq
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
  fn set_and_get_blob() {
    let conn = Connection::open_in_memory().unwrap();
    let db = MetadataDB::new(conn);
    let from_hash = [0;HASHSIZE];
    let from_size = 10;
    db.set_blob(&from_hash, from_size).unwrap();
    let from_time = timeval();
    let (synced, size, last_used) = db.get_blob(&from_hash).unwrap();
    assert_eq!(from_size, size);
    assert_eq!(false, synced);
    assert!(from_time >= last_used);
    std::thread::sleep(std::time::Duration::from_millis(10));
    db.touch_blob(&from_hash);
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
    db.set_blob(&from_hash, 0).unwrap();
    db.mark_synced_blob(&from_hash);
    let (_, _, last_used) = db.get_blob(&from_hash).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(10));
    db.set_blob(&from_hash, 0).unwrap();
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
    db.set_blob(&from_hash1, 0).unwrap();
    db.set_blob(&from_hash2, 0).unwrap();
    db.set_blob(&from_hash3, 0).unwrap();
    db.mark_synced_blob(&from_hash2);
    let to_upload: Vec<BlobHash> = db.to_upload().into();
    assert_eq!(vec![from_hash1, from_hash3], to_upload);
  }
}
