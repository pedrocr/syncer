Normal bugs:
  - Fix broken access to inode 0 on initial startup

Rework the disk sync setup:
  - For the block write path add a cache that only gets written to disk on sync(). This will make sure that we don't write blocks to disk that are fully orphaned by being rewritten immediately (easy to trigger with small writes to a file). To do this do something like:
    - Keep a block cache that new blocks get written to, and read from it in the read path (similar to the entry one right now)
    - On sync(node) write to disk only the blocks that are mentioned in the node
    - On sync_all() do the same for all the nodes in the cache and at the end erase the block cache as whatever is left is now orphaned and will never be needed

Implement a read-only mount of a previous state of the filesystem:
  - Since we don't evict data and keep the historical node->hash relationships the filesystem can be mounted at any point in time just by limiting `MetadataDB::get_node()` to a given moment in time

POSIX stuff:
  - Implement fsync()
  - Implement statfs by just proxying the statfs of the underlying filesystem and some extra metadata from the extended (with remote data) filesystem
  - Maybe consider extended attributes but probably not device files?
