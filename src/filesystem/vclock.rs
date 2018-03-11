use std::cmp::Ordering;
// Not using HashMap because of https://github.com/TyOverby/bincode/issues/230
use std::collections::BTreeMap;
use std::cmp;

#[derive(Debug, PartialEq, Clone)]
pub enum VectorOrdering {
  Less,
  Greater,
  Equal,
  Conflict,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorClock {
  peers: BTreeMap<i64, u64>,
}

impl VectorClock {
  pub fn new() -> Self {
    Self {
      peers: BTreeMap::new(),
    }
  }

  pub fn increment(&mut self, peer: i64) {
    let counter = self.peers.entry(peer).or_insert(0);
    *counter += 1;
  }

  pub fn cmp(&self, other: &VectorClock) -> VectorOrdering {
    let mut keys: Vec<&i64> = self.peers.keys().collect();
    let mut otherkeys: Vec<&i64> = other.peers.keys().collect();
    keys.append(&mut otherkeys);

    let mut ordering = VectorOrdering::Equal;
    for k in keys {
      let v1 = self.peers.get(k).unwrap_or(&0);
      let v2 = other.peers.get(k).unwrap_or(&0);
      let vord = v1.cmp(&v2);
      match (&ordering, vord) {
        (_, Ordering::Equal) => {},
        (&VectorOrdering::Less,    Ordering::Less) => {},
        (&VectorOrdering::Greater, Ordering::Greater) => {},
        (&VectorOrdering::Equal,   Ordering::Less) => {ordering = VectorOrdering::Less},
        (&VectorOrdering::Equal,   Ordering::Greater) => {ordering = VectorOrdering::Greater},
        (&VectorOrdering::Less,    Ordering::Greater) => {return VectorOrdering::Conflict},
        (&VectorOrdering::Greater, Ordering::Less) => {return VectorOrdering::Conflict},
        // This is never reached
        (&VectorOrdering::Conflict, _) => {return VectorOrdering::Conflict},
      }
    }

    ordering
  }

  pub fn merge(&self, other: &VectorClock) -> Self {
    let mut keys: Vec<&i64> = self.peers.keys().collect();
    let mut otherkeys: Vec<&i64> = other.peers.keys().collect();
    keys.append(&mut otherkeys);

    let mut vals = BTreeMap::new();

    for k in keys {
      let v1 = self.peers.get(k).unwrap_or(&0);
      let v2 = other.peers.get(k).unwrap_or(&0);
      vals.insert(*k, *cmp::max(v1,v2));
    }

    Self {
      peers: vals,
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  extern crate bincode;

  #[test]
  fn basic_compare() {
    let mut vclock1 = VectorClock::new();
    vclock1.increment(0);
    let mut vclock2 = VectorClock::new();
    vclock2.increment(0);
    vclock2.increment(0);

    assert_eq!(VectorOrdering::Less, vclock1.cmp(&vclock2));
    assert_eq!(VectorOrdering::Greater, vclock2.cmp(&vclock1));
  }

  #[test]
  fn basic_conflict() {
    let mut vclock1 = VectorClock::new();
    vclock1.increment(0);
    let mut vclock2 = vclock1.clone();

    assert_eq!(VectorOrdering::Equal, vclock1.cmp(&vclock2));
    assert_eq!(VectorOrdering::Equal, vclock2.cmp(&vclock1));

    vclock1.increment(1);
    vclock2.increment(2);

    assert_eq!(VectorOrdering::Conflict, vclock1.cmp(&vclock2));
    assert_eq!(VectorOrdering::Conflict, vclock2.cmp(&vclock1));
  }

  #[test]
  fn serialization_roundtrips() {
    let mut vclock = VectorClock::new();
    vclock.increment(10);
    vclock.increment(0);
    let encoded: Vec<u8> = bincode::serialize(&vclock).unwrap();
    let vclock2: VectorClock = bincode::deserialize(&encoded).unwrap();
    let encoded2: Vec<u8> = bincode::serialize(&vclock2).unwrap();

    assert_eq!(vclock, vclock2);
    assert_eq!(encoded, encoded2);
  }

  #[test]
  fn merge() {
    let mut vclock1 = VectorClock::new();
    vclock1.increment(1);
    vclock1.increment(2);
    let mut vclock2 = VectorClock::new();
    vclock2.increment(2);
    vclock2.increment(2);
    let mut vclock3 = VectorClock::new();
    vclock3.increment(1);
    vclock3.increment(2);
    vclock3.increment(2);

    assert_eq!(vclock3, vclock1.merge(&vclock2));
    assert_eq!(vclock3, vclock2.merge(&vclock1));
  }
}
