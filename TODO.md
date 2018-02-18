Normal bugs
-----------

  - Make sure that `touch()` will create the blob in the database if it doesn't exist in case we crash after having written to disk but before having written to the database

Proper readahead
----------------

  - Readahead is needed to maximize network performance, otherwise we're always blocking for the next file as soon as we reach the end of the 1MB block making for poor network usage
  - A minimal 4-5 block readahead is probably enough since subsequent reads continuously move the window forward
  - To implement this properly a new `RwLock` cache can hold the hashes currently under readhead by anothe thread. Whenever we read we add those blocks to that cache with their own threads fetching and don't block on their completion.
  - On a read that needs to fetch we spinlock on the RwLock cache until the file is available or at least the thread has given up and removed the hash from the cache. In that case if the file still doesn't exist just resume the normal path of fetching during `read()` or `write()` and finally fail in case fetching really is not possible

Proper versioning of the on-disk data
-------------------------------------

  - Currently things like block sizes and `FSEntry` serialization format can change and wreak avoc with an existing setup. As a minimum write a version file to the data dir and bail out on startup if the version doesn't match semver
  - For the block size and hash size settings moving them into a config file in data and startup from there so that the constants in the code are just defaults
  - For the `FSEntry` versioning eventually we want to move into a magic few bytes at the start that identify the version and then just keep `FSEntryV1` and `FSEntryV2` structs around with implementations to turn them into the proper entry so that upgrades to the on-disk values are seamless

Compressed Data
---------------

  - Compressing blobs that are large enough with a fast compressor might save quite a bit of space and bandwidth for very little CPU
  - For this to work on-disk blobs should have a few magic bytes to indicate if they are compressed or not just like for on-disk structures they should indicate the version.

Multi-master read/write
-----------------------

  - syncer would be most useful if you could just do multi-master read/write so you could use to sync between several machines. It may be possible to do it with the rsync setup intact
  - Have each machine have an ID (a random 64bit number will not collide so that should work)
  - New nodes get a 128bit number as key that has the sequential 64bit number as now and the machine ID attached (so that new node creation doesn't require synchronization)
  - The `nodes` table includes a new column with a vector clock that gets initialized with just the originating machine's ID and 1
  - Whenever a node is updated bump the vector clock for whatever machine you are
  - Push all node changes to an ID named file on the server (with `rsync --append`) but only do it after pushing all the blobs that are referred by the node (so that another machine doesn't end up fetching a node for which there is no content yet and having an inconsistent state)
  - Fetch all node changes from other ID's by running rsync in the other direction as well
  - Process all new nodes, if the vector clock says all is in order just add them to the filesystem if not do the merge between nodes. Some parts of `FSEntry` can be merged (`children` and `xattrs` mostly), for everything else just have a stable rule on who wins (maybe the machine with the highest ID)
  - This should give you a read-write filesystem where all machines arrive on the same current state without intelligence in the central server (ideal for a NAS). It may result in some weirdness with files though (e.g., hard-linked directories) and I haven't thought about all those possible implications
    - To fix the hard-link problem a solution would be to never reuse IDs on rename(), instead always create a new ID that points to the same hash. That way if machine A does `rename("foo/", "bar/")` and machine B does `rename("foo/", "baz/")` they get two different directories with the same content. The problem with this is that a deep copy is needed, refreshing ID's for all subdirectories as well. IDs are cheap though as the actual data is left unchanged.

Read-only mount of a previous state of the filesystem
-----------------------------------------------------

  - Since we don't evict data and keep the historical node->hash relationships the filesystem can be mounted at any point in time just by limiting `MetadataDB::get_node()` to a given moment in time

POSIX stuff
-----------

  - Implement statfs by just proxying the statfs of the underlying filesystem and some extra metadata from the extended (with remote data) filesystem
