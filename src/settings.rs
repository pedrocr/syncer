// These are tunables that can be changed without much issue

// How many blocks to read ahead of the current one when we fetch one from disk
pub const READAHEAD: usize = 800;

// How many blobs to fetch at once for upload
pub const TO_UPLOAD: usize = 800;

// How many blobs to fetch at once for delete
pub const TO_DELETE: usize = 800;

// From now on these can be changed but will make the on-disk format incompatible
// Making them per-repository in the future may make sense for some

// 20 bytes are probably more than enough for safety
pub const HASHSIZE: usize = 20;

// Smaller blocks mean better deduplication but make for much slower performance
pub const BLKSIZE: usize = 4096;

