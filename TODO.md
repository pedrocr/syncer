Normal bugs:
  - Figure out why sqlite is so damn slow at inserts now that the write path is unbearably slow

Rework the disk sync setup:
  - For the block write path add a cache that only gets written to disk on sync(). This will make sure that we don't write blocks to disk that are fully orphaned by being rewritten immediately (easy to trigger with small writes to a file). To do this do something like:
    - Keep a block cache that new blocks get written to, and read from it in the read path (similar to the entry one right now)
    - On sync(node) write to disk only the blocks that are mentioned in the node
    - On sync_all() do the same for all the nodes in the cache and at the end erase the block cache as whatever is left is now orphaned and will never be needed

Smaller stuff:
  - Implement statfs by just proxying the statfs of the underlying filesystem and some extra metadata from the extended (with remote data) filesystem
  - Maybe consider extended attributes but probably not device files?
