Just your standard bugs:
  - Rename is just hopelessly broken for directories as it leaves all the children orphans
  - Hard links don't work (bug report to fuse_mt submitted)
  - open(O_CREAT) doesn't work (suspect fuse_mt again but haven't sent a bug report)
  - unlinking files doesn't delete nodes so no space is reclaimed

To do to get the basic POSIX ramfs working:
  - Implement proper opendir/readdir
  - Maybe consider extended attributes but probably not device files?
