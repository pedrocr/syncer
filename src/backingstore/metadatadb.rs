extern crate rusqlite;
extern crate libc;
extern crate hex;
extern crate time;

use super::blobstorage::*;
use super::{NodeInfo, NodeId};
use crate::settings::*;
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
      peernum         INTEGER NOT NULL,
      id              INTEGER NOT NULL,
      hash            TEXT NOT NULL,
      creation        INTEGER NOT NULL,
      synced          INTEGER NOT NULL,
      UNIQUE (peernum, id, hash, creation) ON CONFLICT IGNORE
    )", &[]).unwrap();

    connection.execute("CREATE TABLE IF NOT EXISTS blobs (
      hash            TEXT PRIMARY KEY,
      synced          INTEGER NOT NULL,
      present         INTEGER NOT NULL,
      size            INTEGER NOT NULL,
      last_use        INTEGER NOT NULL
    )", &[]).unwrap();

    connection.execute("CREATE TABLE IF NOT EXISTS peers (
      id              INTEGER PRIMARY KEY,
      offset          INTEGER NOT NULL
    )", &[]).unwrap();

    connection.execute("CREATE INDEX IF NOT EXISTS node_id
                        ON nodes (peernum, id)", &[]).unwrap();

    connection.execute("CREATE INDEX IF NOT EXISTS blob_upload
                        ON blobs (synced)", &[]).unwrap();

    connection.execute("CREATE INDEX IF NOT EXISTS blob_delete
                        ON blobs (synced, present, last_use)", &[]).unwrap();

    Self {
      connection: Mutex::new(connection),
    }
  }

  pub fn max_node(&self, peernum: i64) -> Result<i64, c_int> {
    let conn = self.connection.lock().unwrap();
    let node: i64 = dberror_return!(conn.query_row(
      "SELECT COALESCE(MAX(id), 0) FROM nodes WHERE peernum=?1",
      &[&peernum], |row| row.get(0)));
    Ok(node)
  }

  pub fn node_exists(&self, node: NodeId) -> Result<bool, c_int> {
    let conn = self.connection.lock().unwrap();
    let count: i64 = dberror_return!(conn.query_row(
      "SELECT count(*) FROM nodes WHERE peernum=?1 AND id=?2 LIMIT 1",
      &[&node.0, &node.1], |row| row.get(0)));
    Ok(count > 0)
  }

  pub fn node_exists_long(&self, node: NodeId, hash: &BlobHash, creation: i64) -> Result<bool, c_int> {
    let conn = self.connection.lock().unwrap();
    let count: i64 = dberror_return!(conn.query_row(
      "SELECT count(*) FROM nodes WHERE peernum=?1 AND id=?2 AND hash=?3 AND creation=?4 LIMIT 1",
      &[&node.0, &node.1, &(hex::encode(hash)), &creation], |row| row.get(0)));
    Ok(count > 0)
  }

  pub fn get_node(&self, node: NodeId) -> Result<BlobHash, c_int> {
    let conn = self.connection.lock().unwrap();
    let hash: String = dberror_return!(conn.query_row(
      "SELECT hash FROM nodes WHERE peernum=?1 AND id=?2 ORDER BY rowid DESC LIMIT 1",
      &[&node.0, &node.1], |row| row.get(0)));
    Ok(Self::hash_from_string(hash))
  }

  pub fn get_earlier_node(&self, node: NodeId, maxrowid: i64) -> Result<(i64, BlobHash), c_int> {
    let conn = self.connection.lock().unwrap();
    let (row, hash): (i64, String) = dberror_return!(conn.query_row(
      "SELECT rowid, hash FROM nodes WHERE peernum=?1 AND id=?2 AND rowid < ?3 ORDER BY rowid DESC LIMIT 1",
      &[&node.0, &node.1, &maxrowid], |row| (row.get(0), row.get(1))));
    Ok((row, Self::hash_from_string(hash)))
  }

  pub fn set_peer(&self, id: i64, offset: u64) -> Result<(), c_int> {
    let conn = self.connection.lock().unwrap();
    dberror_return!(conn.execute(
      "INSERT OR REPLACE INTO peers (id, offset) VALUES (?1, ?2)",
      &[&id, &(offset as i64)]));
    Ok(())
  }

  pub fn get_peer(&self, id: i64) -> Result<u64, c_int> {
    let conn = self.connection.lock().unwrap();
    let val: i64 = dberror_return!(conn.query_row(
      "SELECT COALESCE(SUM(offset), 0) FROM peers WHERE id=?1",
      &[&id], |row| row.get(0)));
    Ok(val as u64)
  }

  pub fn set_node(&self, node: NodeId, hash: &BlobHash, creation: i64) -> Result<(), c_int> {
    let conn = self.connection.lock().unwrap();
    dberror_return!(conn.execute(
      "INSERT INTO nodes (peernum, id, hash, creation, synced) VALUES (?1, ?2, ?3, ?4, 0)",
      &[&node.0, &node.1, &(hex::encode(hash)), &creation]));
    Ok(())
  }

  pub fn set_node_behind(&self, node: NodeId, hash: &BlobHash, creation: i64) -> Result<(), c_int> {
    let mut conn = self.connection.lock().unwrap();
    let tran = conn.transaction().unwrap();
    let (rowid, oldhash, oldcreation): (i64, String, i64) = dberror_return!(tran.query_row(
      "SELECT rowid, hash, creation FROM nodes WHERE peernum=?1 AND id=?2 ORDER BY rowid DESC LIMIT 1",
      &[&node.0, &node.1], |row| (row.get(0), row.get(1), row.get(2))));
    dberror_return!(tran.execute(
      "DELETE FROM nodes WHERE rowid=?1",
      &[&rowid]));
    dberror_return!(tran.execute(
      "INSERT INTO nodes (peernum, id, hash, creation, synced) VALUES (?1, ?2, ?3, ?4, 0)",
      &[&node.0, &node.1, &(hex::encode(hash)), &creation]));
    dberror_return!(tran.execute(
      "INSERT INTO nodes (peernum, id, hash, creation, synced) VALUES (?1, ?2, ?3, ?4, 0)",
      &[&node.0, &node.1, &oldhash, &oldcreation]));
    tran.commit().unwrap();
    Ok(())
  }

  pub fn to_upload_nodes(&self) -> Vec<(i64, NodeInfo)> {
    let conn = self.connection.lock().unwrap();
    let mut stmt = conn.prepare(&format!(
      "SELECT nodes.rowid, nodes.peernum, nodes.id, nodes.hash, nodes.creation
       FROM nodes JOIN blobs ON nodes.hash = blobs.hash
       WHERE nodes.synced = 0 AND blobs.synced = 1
       ORDER BY nodes.rowid LIMIT {}", TO_UPLOAD_NODES)).unwrap();
    let iter = stmt.query_map(&[], |row| {
      (row.get(0),
      NodeInfo {
        id: (row.get(1), row.get(2)),
        hash: Self::hash_from_string(row.get(3)),
        creation: row.get(4),
      })
    }).unwrap();
    let mut vals = Vec::new();
    for val in iter {
      vals.push(val.unwrap());
    }
    vals
  }

  pub fn mark_synced_nodes(&self, vals: &[i64]) {
    let mut conn = self.connection.lock().unwrap();
    let tran = conn.transaction().unwrap();
    for rowid in vals {
      dberror_test!(tran.execute(
        "UPDATE OR IGNORE nodes SET synced = 1 WHERE rowid = ?1",
        &[rowid]));
    }
    tran.commit().unwrap();
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
    where I: Iterator<Item = (BlobHash, (i64, usize))> {
    let mut conn = self.connection.lock().unwrap();
    let tran = conn.transaction().unwrap();
    for (hash, (time, size)) in vals {
      dberror_test!(tran.execute(
        "INSERT OR REPLACE INTO blobs (hash, present, last_use, size, synced)
         VALUES (?1, 1, ?2, ?3,
           COALESCE((SELECT synced FROM blobs WHERE hash = ?1), 0)
         );",
         &[&(hex::encode(hash)), &time, &(size as i64)]));
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
    let mut stmt = conn.prepare(&format!(
      "SELECT hash FROM blobs WHERE synced = 0 ORDER BY rowid LIMIT {}", TO_UPLOAD)).unwrap();
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
    let mut stmt = conn.prepare(&format!(
      "SELECT hash, size FROM blobs WHERE synced = 1 AND present = 1 AND size > {}
       ORDER BY last_use ASC LIMIT {}", KEEP_UP_TO_SIZE, TO_DELETE)).unwrap();
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
  use std::i64;

  #[test]
  fn set_and_get_node() {
    let conn = Connection::open_in_memory().unwrap();
    let db = MetadataDB::new(conn);
    assert_eq!(db.node_exists((0,0)).unwrap(), false);
    let from_hash = [0;HASHSIZE];
    db.set_node((0,0), &from_hash, timeval()).unwrap();
    assert_eq!(db.node_exists((0,0)).unwrap(), true);
    let hash = db.get_node((0,0)).unwrap();
    assert_eq!(from_hash, hash);
  }

  #[test]
  fn set_and_reset_node() {
    let conn = Connection::open_in_memory().unwrap();
    let db = MetadataDB::new(conn);
    let from_hash = [0;HASHSIZE];
    db.set_node((0,0), &from_hash, timeval()).unwrap();
    let from_hash = [1;HASHSIZE];
    db.set_node((0,0), &from_hash, timeval()).unwrap();
    let hash = db.get_node((0,0)).unwrap();
    assert_eq!(from_hash, hash);
  }

  #[test]
  fn double_set_node() {
    let conn = Connection::open_in_memory().unwrap();
    let db = MetadataDB::new(conn);
    assert_eq!(db.node_exists((0,0)).unwrap(), false);
    let from_hash = [0;HASHSIZE];
    db.set_blob(&from_hash, 0);
    db.mark_synced_blob(&from_hash);
    let time = timeval();
    db.set_node((0,0), &from_hash, time).unwrap();
    db.set_node((0,0), &from_hash, time).unwrap();
    assert_eq!(1, db.to_upload_nodes().len());
  }

  #[test]
  fn get_earlier_node() {
    let conn = Connection::open_in_memory().unwrap();
    let db = MetadataDB::new(conn);
    let from_hash1 = [1;HASHSIZE];
    let from_hash2 = [2;HASHSIZE];
    let from_hash3 = [3;HASHSIZE];
    let time = timeval();
    db.set_node((0,0), &from_hash1, time).unwrap();
    db.set_node((0,0), &from_hash2, time).unwrap();
    db.set_node((0,0), &from_hash3, time).unwrap();

    let (row, hash) = db.get_earlier_node((0,0), i64::MAX).unwrap();
    assert_eq!(from_hash3, hash);
    let (row, hash) = db.get_earlier_node((0,0), row).unwrap();
    assert_eq!(from_hash2, hash);
    let (row, hash) = db.get_earlier_node((0,0), row).unwrap();
    assert_eq!(from_hash1, hash);
    assert!(db.get_earlier_node((0,0), row).is_err());
  }

  #[test]
  fn node_exists_long() {
    let conn = Connection::open_in_memory().unwrap();
    let db = MetadataDB::new(conn);
    let from_hash1 = [1;HASHSIZE];
    let from_hash2 = [2;HASHSIZE];
    let time1 = timeval();
    let time2 = time1+1;
    assert_eq!(db.node_exists_long((0,0), &from_hash1, time1).unwrap(), false);
    db.set_node((0,0), &from_hash2, time2).unwrap();
    assert_eq!(db.node_exists_long((0,0), &from_hash1, time1).unwrap(), false);
    assert_eq!(db.node_exists_long((0,0), &from_hash2, time1).unwrap(), false);
    assert_eq!(db.node_exists_long((0,0), &from_hash1, time2).unwrap(), false);
    assert_eq!(db.node_exists_long((0,0), &from_hash2, time2).unwrap(), true);
  }

  #[test]
  fn set_node_behind() {
    let conn = Connection::open_in_memory().unwrap();
    let db = MetadataDB::new(conn);
    assert_eq!(db.node_exists((0,0)).unwrap(), false);
    let from_hash1 = [1;HASHSIZE];
    let from_hash2 = [2;HASHSIZE];
    db.set_node((0,0), &from_hash1, timeval()).unwrap();
    db.set_node_behind((0,0), &from_hash2, timeval()).unwrap();
    assert_eq!(from_hash1, db.get_node((0,0)).unwrap());
  }

  #[test]
  fn maxnode() {
    let conn = Connection::open_in_memory().unwrap();
    let db = MetadataDB::new(conn);
    assert_eq!(0, db.max_node(0).unwrap());
    let from_hash = [0;HASHSIZE];
    db.set_node((0,5), &from_hash, timeval()).unwrap();
    assert_eq!(5, db.max_node(0).unwrap());
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
    let mut vals = vec![(from_hash, (timeval(),10))];
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
    db.set_blob(&from_hash1, 100000);
    db.set_blob(&from_hash2, 200000);
    db.set_blob(&from_hash3, 300000);
    db.mark_synced_blob(&from_hash2);
    db.mark_synced_blob(&from_hash3);
    assert_eq!(vec![(from_hash2, 200000), (from_hash3, 300000)], db.to_delete());
    db.mark_deleted_blobs(&[from_hash2], true);
    assert_eq!(vec![(from_hash3, 300000)], db.to_delete());
    db.mark_deleted_blobs(&[from_hash2], false);
    assert_eq!(vec![(from_hash2, 200000), (from_hash3, 300000)], db.to_delete());
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

  #[test]
  fn touch_marks_local() {
    let conn = Connection::open_in_memory().unwrap();
    let db = MetadataDB::new(conn);
    let from_hash = [0;HASHSIZE];
    let from_size = 10;
    assert_eq!(0, db.localbytes());
    db.set_blob(&from_hash, from_size);
    assert_eq!(10, db.localbytes());
    db.mark_deleted_blobs(&[from_hash], true);
    assert_eq!(0, db.localbytes());
    let mut vals = vec![(from_hash, (timeval(),10))];
    db.touch_blobs(vals.drain(..));
    assert_eq!(10, db.localbytes());
  }

  #[test]
  fn to_upload_nodes() {
    let conn = Connection::open_in_memory().unwrap();
    let db = MetadataDB::new(conn);
    let from_hash = [1;HASHSIZE];
    db.set_blob(&from_hash, 0);
    db.set_node((0,0), &from_hash, timeval()).unwrap();

    // When we haven't synced any blobs there are no nodes to upload
    let to_upload = db.to_upload_nodes();
    assert_eq!(0, to_upload.len());

    // After we sync the blob we can upload
    db.mark_synced_blob(&from_hash);
    let to_upload = db.to_upload_nodes();
    assert_eq!(1, to_upload.len());
    assert_eq!(from_hash, to_upload[0].1.hash);

    // After we sync the node there's again nothing left
    db.mark_synced_nodes(&[to_upload[0].0]);
    let to_upload = db.to_upload_nodes();
    assert_eq!(0, to_upload.len());
  }

  #[test]
  fn touch_creates() {
    let conn = Connection::open_in_memory().unwrap();
    let db = MetadataDB::new(conn);
    let from_hash = [0;HASHSIZE];
    assert_eq!(0, db.localbytes());
    let mut vals = vec![(from_hash, (timeval(), 10))];
    db.touch_blobs(vals.drain(..));
    assert_eq!(10, db.localbytes());
  }

  #[test]
  fn set_and_get_peer() {
    let conn = Connection::open_in_memory().unwrap();
    let db = MetadataDB::new(conn);
    assert_eq!(0, db.get_peer(0).unwrap());
    db.set_peer(0, 0).unwrap();
    assert_eq!(0, db.get_peer(0).unwrap());
    db.set_peer(1, 10).unwrap();
    assert_eq!(10, db.get_peer(1).unwrap());
    db.set_peer(0, 10).unwrap();
    assert_eq!(10, db.get_peer(0).unwrap());
  }
}
