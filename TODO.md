Rework the disk sync setup:
  - For the block write path add a cache that only gets written to disk on sync(). This will make sure that we don't write blocks to disk that are fully orphaned by being rewritten immediately (easy to trigger with small writes to a file). To do this do something like:
    - Keep a block cache that new blocks get written to, and read from it in the read path (similar to the entry one right now)
    - On sync(node) write to disk only the blocks that are mentioned in the node
    - On sync_all() do the same for all the nodes in the cache and at the end erase the block cache as whatever is left is now orphaned and will never be needed

Get the basic network sync working:
  - Add a new sqlite table that keeps track of blocks by having hash as the primary key and keeping extra metadata (has it been synced to the remote, how recently has it been used, what size it is)
  - On sync of a file to disk add it to a queue to get shipped off to the remote and do that in a separate thread. (To guard against shutdown that separate thread will have to be fed with a database query on startup)
  - On the read(hash) path whenever a block is missing block while downloading it from the remote (be careful to do it without holding any locks)

Smaller stuff:
  - Implement statfs by just proxying the statfs of the underlying filesystem and some extra metadata from the extended (with remote data) filesystem
  - Maybe consider extended attributes but probably not device files?
