use std::sync::{RwLock, RwLockWriteGuard, RwLockReadGuard};
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub struct RwHashes<K,V> {
  buckets: Vec<RwLock<HashMap<K,V>>>,
  mask: u64,
}

impl<K: Hash + Eq,V> RwHashes<K,V> {
  pub fn new(bits: usize) -> Self {
    let buckets = {
      let mut v = Vec::new();
      for _ in 0..(2usize.pow(bits as u32)) {
        v.push(RwLock::new(HashMap::new()));
      }
      v
    };

    Self {
      buckets,
      mask: (1u64 << bits) - 1,
    }
  }

  fn get_bucket(&self, key: &K) -> usize {
    let mut s = DefaultHasher::new();
    key.hash(&mut s);
    (s.finish() & self.mask) as usize
  }

  pub fn read(&self, key: &K) -> RwLockReadGuard<HashMap<K,V>> {
    self.buckets[self.get_bucket(key)].read().unwrap()
  }

  pub fn write(&self, key: &K) -> RwLockWriteGuard<HashMap<K,V>> {
    self.buckets[self.get_bucket(key)].write().unwrap()
  }

  pub fn write_pos(&self, index: usize) -> RwLockWriteGuard<HashMap<K,V>> {
    self.buckets[index].write().unwrap()
  }

  pub fn len(&self) -> usize {
    self.buckets.len()
  }
}
