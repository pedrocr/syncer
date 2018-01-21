Just your standard bugs:
  - unlinking/truncating files doesn't delete nodes so no space is reclaimed

To do to get the basic POSIX ramfs working:
  - Implement statfs
  - Maybe consider extended attributes but probably not device files?
