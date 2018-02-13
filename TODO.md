Normal bugs:
 - Figure out why rsync is so slow at fetching files from the network

Make metadata much less likely to be evicted
  - Listing directories should be generally faster than opening files. Right now since directories end up being small blobs this is particularly unfriendly as fetching a bunch of small blobs is very slow
  - Either completely exclude metadata from being evicted or just make it very unlikely by putting it all after files
  - In either case a new column is needed in the blobs table to signal the type of blob and thus allowing `to_delete()` to exclude or sort based on it

Figure out how to reduce lock contention:
  - Currently the node and blob hashes have a lot of lock contention under write loads
  - In reality unless you're actually reading/writing to the same file from several threads (very unlikely) there's probably not much actual contention for resources but since it all goes through the same `RwLock` it bogs down substantially
  - A naive (but maybe good enough solution) is to just replace the single `RwLock` with a `[RwLock;256]` keyed by 8 bytes of the hash of `node`. That way only if you end up concurrently writing to two files that share the same lock do you get the issue. It's possible even `[RwLock;65536]` wouldn't be too much of a memory hog.

Implement a read-only mount of a previous state of the filesystem:
  - Since we don't evict data and keep the historical node->hash relationships the filesystem can be mounted at any point in time just by limiting `MetadataDB::get_node()` to a given moment in time

POSIX stuff:
  - Implement fsync()
  - Implement statfs by just proxying the statfs of the underlying filesystem and some extra metadata from the extended (with remote data) filesystem
  - Maybe consider extended attributes but probably not device files?
