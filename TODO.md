Normal bugs:
  - Make sure that `touch()` will create the blob in the database if it doesn't exist in case we crash after having written to disk but before having written to the database

Proper readahead:
  - Readahead is needed to maximize network performance, otherwise we're always blocking for the next file as soon as we reach the end of the 1MB block making for poor network usage
  - A minimal 4-5 block readahead is probably enough since subsequent reads continuously move the window forward
  - To implement this properly a new `RwLock` cache can hold the hashes currently under readhead by anothe thread. Whenever we read we add those blocks to that cache with their own threads fetching and don't block on their completion.
  - On a read that needs to fetch we spinlock on the RwLock cache until the file is available or at least the thread has given up and removed the hash from the cache. In that case if the file still doesn't exist just resume the normal path of fetching during `read()` or `write()` and finally fail in case fetching really is not possible

Proper versioning of the on-disk data:
  - Currently things like block sizes and `FSEntry` serialization format can change and wreak avoc with an existing setup. As a minimum write a version file to the data dir and bail out on startup if the version doesn't match semver
  - For the block size and hash size settings moving them into a config file in data and startup from there so that the constants in the code are just defaults
  - For the `FSEntry` versioning eventually we want to move into a magic few bytes at the start that identify the version and then just keep `FSEntryV1` and `FSEntryV2` structs around with implementations to turn them into the proper entry so that upgrades to the on-disk values are seamless

Implement a read-only mount of a previous state of the filesystem:
  - Since we don't evict data and keep the historical node->hash relationships the filesystem can be mounted at any point in time just by limiting `MetadataDB::get_node()` to a given moment in time

POSIX stuff:
  - Implement fsync()
  - Implement statfs by just proxying the statfs of the underlying filesystem and some extra metadata from the extended (with remote data) filesystem
  - Maybe consider extended attributes but probably not device files?
