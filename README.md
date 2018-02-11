# syncer

**WARNING: This is highly experimental and might eat your data. Make sure you have good backups before you test it.**

This is a filesystem that allows you to keep a seamless local view of a very large repository of files while only really having a much smaller local cache. It's meant for situations where you have too small of a disk to hold the full collection but instead would like to fetch data from a remote server on demand. The use case it was built for was having a very large collection of media (e.g., a multi-terabyte photo collection) and wanting to be able to seamlessly access it at any time on a laptop that only has a few GBs of space.

syncer is built as a FUSE filesystem so it presents a regular POSIX interface that any app should be able to use. Files are internally split into blocks and hashed. Those blocks get uploaded to any rsync end point you want (usually an SSH server). Then when the local storage exceeds the limited amount the least recently used blocks get evicted. They get brought back into local storage on demand by fetching them from the remote server again.

Current State
-------------

The basic program works and syncs to a remote rsync/ssh server. This should be enough for a photo collection which is mostly a set of fixed files that don't get changed a lot. But this is still highly experimental and might eat your data. The basic existing features are:

  - The standard POSIX filesystem works and persists to disk (tested on Linux, might work on OSX as well)
  - Pushing to the remote server and pulling on demand works as well
  - The total speed should be fast enough for reasonable usage but it certainly won't be a speed daemon

Still on the TODO list:

  - Stress test and build a repeatable testing set for all POSIX operations
  - Tune for performance more thoroughly (for example very small sequential writes are a pathological case right now)
  - Implement a better sync endpoint than just rsync/ssh as setting up those connections repeatedly is very time consuming. A simple daemon to send/receive blocks that maybe even allows multi-server failover and redundancy would be nice. Or maybe something like the S3 protocol would fit.
  - Allow marking certain files/directories as allways available locally so you can set it on the thumbnail dir of a photo application and get fast browsing at all times
  - Expose a Time Machine like interface showing read-only snapshots of the filesytem (already present in the data but not exposed) 
  - Figure out a good way to evict old data (currently all history is kept)

Usage
-----

To install or upgrade just do:

```sh

$ cargo install -f syncer
```

To start the filesystem do something like:

```sh

$ syncer data someserver:~/blobs/ mnt 1000
```

That will give you a filesystem at `mnt` that you can use normally. The data for it comes from the `data` folder locally and the server. At most syncer will try to use 1GB locally and then fetch from server when needed.

Contributing
------------

Bug reports and pull requests welcome at https://github.com/pedrocr/syncer

Meet us at #chimper on irc.mozilla.org if you need to discuss a feature or issue in detail or even just for general chat.
