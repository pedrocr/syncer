// These are tunables that can be changed without much issue

// How many blobs to fetch at once for upload
pub const TO_UPLOAD: usize = 4;

// How many blobs to fetch at once for delete
pub const TO_DELETE: usize = 100;

// How large of a file to never evict from local cache
pub const KEEP_UP_TO_SIZE: usize = 65536;

// From now on these can be changed but will make the on-disk format incompatible
// Making them per-repository in the future may make sense for some

// 20 bytes are probably more than enough for safety
pub const HASHSIZE: usize = 20;

// Smaller blocks mean better deduplication but make for much slower performance
// Disks use base 10 so use 1MB instead of 1MiB
pub const BLKSIZE: usize = 1000000;

