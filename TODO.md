Just your standard bugs:
  - Hard links don't work (bug report to fuse_mt submitted)
  - unlinking/truncating files doesn't delete nodes so no space is reclaimed

To do to get the basic POSIX ramfs working:
  - Implement statfs
  - Implement proper access control
  - Maybe consider extended attributes but probably not device files?
