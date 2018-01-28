extern crate rusqlite;
extern crate libc;
extern crate hex;
use self::rusqlite::Connection;
use std::sync::Mutex;
use self::libc::c_int;
use super::blobstorage::*;

pub struct MetadataDB {
  connection: Mutex<Connection>,
}

impl MetadataDB {
  pub fn new(connection: Connection) -> Self {
    connection.execute("CREATE TABLE IF NOT EXISTS nodes (
                        id              INTEGER PRIMARY KEY,
                        hash            TEXT NOT NULL
                        )", &[]).unwrap();

    Self {
      connection: Mutex::new(connection),
    }
  }

  pub fn get(&self, node: u64) -> Result<BlobHash, c_int> {
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

  pub fn set(&self, node: u64, hash: &BlobHash) -> Result<(), c_int> {
    let conn = self.connection.lock().unwrap();
    let hash = hash.to_vec();
    match conn.execute("INSERT OR REPLACE INTO nodes (id, hash) VALUES (?1, ?2)",
                 &[&(node as i64), &(hex::encode(hash))]) {
      Ok(_) => Ok(()),
      Err(_) => return Err(libc::EIO),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn set_and_get() {
    let conn = Connection::open_in_memory().unwrap();
    let db = MetadataDB::new(conn);
    let from_hash = [0;HASHSIZE];
    db.set(0, &from_hash).unwrap();
    let hash = db.get(0).unwrap();
    assert_eq!(from_hash, hash);
  }
}
