extern crate rusqlite;
extern crate libc;
use self::rusqlite::Connection;
use std::path::PathBuf;
use std::sync::Mutex;
use self::libc::c_int;
use super::blobstorage::*;

pub struct MetadataDB {
  connection: Mutex<Connection>,
}

impl MetadataDB {
  pub fn new(path: &str) -> Self {
    let mut file = PathBuf::from(path);
    file.push("metadata.sqlite3");
    let connection = Connection::open(&file).unwrap();

    connection.execute("CREATE TABLE IF NOT EXISTS nodes (
                        id              INTEGER PRIMARY KEY,
                        hash            BLOB
                        )", &[]).unwrap();

    Self {
      connection: Mutex::new(connection),
    }
  }

  pub fn get(&self, node: u64) -> Result<BlobHash, c_int> {
    let conn = self.connection.lock().unwrap();
    let hash: Vec<u8> = match conn.query_row("SELECT hash FROM nodes WHERE id=?1", 
                                             &[&(node as i64)], |row| row.get(0)) {
      Ok(hash) => hash,
      Err(_) => return Err(libc::EIO),
    };
    assert!(hash.len() == HASHSIZE);
    let mut hasharray = [0; HASHSIZE];
    for i in 0..HASHSIZE {
      hasharray[i] = hash[i];
    }
    Ok(hasharray)
  }

  pub fn set(&self, node: u64, hash: &BlobHash) -> Result<(), c_int> {
    let conn = self.connection.lock().unwrap();
    let hash = hash.to_vec();
    conn.execute("INSERT OR REPLACE INTO nodes (id, hash) VALUES (?1, ?2)",
                 &[&(node as i64), &hash]).unwrap();
    Ok(())
  }
}
