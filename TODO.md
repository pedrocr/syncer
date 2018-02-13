Normal bugs:
  - Fix broken access to inode 0 on initial startup

Implement a read-only mount of a previous state of the filesystem:
  - Since we don't evict data and keep the historical node->hash relationships the filesystem can be mounted at any point in time just by limiting `MetadataDB::get_node()` to a given moment in time

POSIX stuff:
  - Implement fsync()
  - Implement statfs by just proxying the statfs of the underlying filesystem and some extra metadata from the extended (with remote data) filesystem
  - Maybe consider extended attributes but probably not device files?
