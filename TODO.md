Normal bugs
-----------

  - Make sure that `touch()` will create the blob in the database if it doesn't exist in case we crash after having written to disk but before having written to the database

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
  - New nodes get a 128bit number as key composed of the current sequential 64bit number as high bits and the machine ID as low bits so that new node creation doesn't require synchronization. To initialize the count just take the highest number from the DB like now and shift it 64bits right
  - The `nodes` table includes a new column with a vector clock that gets initialized with just the originating machine's ID and 1
  - Whenever a node is updated bump the vector clock for whatever machine you are
  - Push all node changes to an ID named file on the server (with `rsync --append`) but only do it after pushing all the blobs that are referred by the node (so that another machine doesn't end up fetching a node for which there is no content yet and having an inconsistent state)
  - Fetch all node changes from other ID's by running rsync in the other direction as well
  - Process all new nodes. If the vector clock says all is in order just add them to the filesystem. If the vector clock signals a conflict do a three way merge by going back to the latest entry that is shared between the current on-disk node and the new one. For the cases where an actual conflict exists (both changed the same value) pick the one with the highest timestamp and if all else fails the machine with the highest ID. For `children`, `xattrs` and `blocks` a proper three way merge is also possible.
  - To handle renames properly disallow hardlinks in the filesystem and add a parent field to the `nodes` table. Whenever a new node gets written from a sync iterate all its child nodes (files or directories) and if the parent node in the database is not the same and still has it as a child remove it.

Read-only mount of a previous state of the filesystem
-----------------------------------------------------

  - Since we don't evict data and keep the historical node->hash relationships the filesystem can be mounted at any point in time just by limiting `MetadataDB::get_node()` to a given moment in time

POSIX stuff
-----------

  - Implement statfs by just proxying the statfs of the underlying filesystem and some extra metadata from the extended (with remote data) filesystem
