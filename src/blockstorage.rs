pub const BLKSIZE: usize = 4096;

#[derive(Clone)]
pub struct FSBlock {
  pub data: [u8; BLKSIZE],
}

impl FSBlock {
  pub fn new() -> FSBlock {
    FSBlock {
      data: [0; BLKSIZE],
    }
  }
}

