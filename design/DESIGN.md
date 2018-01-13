This is just the basic design for future reference of a filesystem with the following characteristics:
- Normal UNIX semantics from the point of view of applications
- Network distributed with a single ReadWrite master and however many read only storage slaves as you want (can be generalized later)
- The master doesn't need to hold all the data physically at all times (i.e., can be used as a 10GB cache to a 10TB filesystem)


For storage this is a simple filesystem with deduplication and space saving
- Split files into blocks of size S chosen to not waste too much space when stored as a file in normal filesystems
- For each block hash it and store it in the filesystem by the hash name
- For metadata use an sqlite3 database:
  - files are a hash of contents, a list of block hashes plus the normal metadata (path, permissions, etc)
  - each block hash has a reference counter, a last use count and a weaker much simpler hash (more on that later)
- Expose these blocks as a normal unix filesystem with fuse
- Whenever new blocks are created ship them off to the slaves
- When the master allocated space evict the least recently used blocks that have already been backed up to slaves
- When you need a block that you don't have fetch it from a slave based on the content hash
- For performance to not be horrible and continuously thrash the filesystem keep a memory cache of in flight blocks that are only hashed and commited to disk/slave every so often so that we don't end up with the pathological case of a bunch of sequential 1 byte writes generating S new blocks per existing block

This gives you the basic setup but a transfers from master to slaves can be sped up with an rsync style protocol
- Take the new file (not block) that you now have and iterate it with an S wide rolling window byte by byte, calculating the much simpler rsync style checksum, ignore the blocks that are already transferred to slaves
- For each cheap checksum check if there's an existing block that matches and if so make sure it is really the same by calculating the full checksum
- Send a set of instructions to create the new blocks to the slaves based on existing blocks (e.g., Block A is created by first using these 100 bytes and then using the other S-100 bytes starting from position 100 of block B)
- For extra credit maybe the previous step can actually be a block storage format to save space by storing some blocks as compositions of other blocks

Extra work would be needed to get this to work with multi-master setups but maybe not too much. Maybe just doing some vector clocks to detect conflicts and surface them as extra files would be enough.

For applications that want to access files by hash a different folder structure or direct API could be added. This together with special append-only files could be used to implement the other idea for a distributed photo editing database.
