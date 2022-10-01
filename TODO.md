# TODO

There are things we need to address before we are production ready.

## Inode counter contention

We currently use a single key for inodes number generation - this is a huge point of contention.

## Nchild contention

Directories currently keep a count of children - this count is a source of contention.

## Permission checks

Permission checks are all done via fuse - however because HAFS is a distributed filesystem there
are still TOCTOU races - we need to do our own permission checks within transactions to be sure.

## Loop checks

In certain situations it is possible to move a directory inside itself and it will become unreachable,
if there is an efficient way to avoid this we could consider it - though it might not be worth fixing.

## Bulk data directories

We need to design a system for bulk data - it is acceptable to disable some filesystem functionaility for these
directories.

One thought is an include an xattr that makes a directory and its children backed by s3/rados/... objects. These
files will probably only be writable in sequential runs.

## Multiple mounts

Currently there is only a single large filesystem - we could support multiple mount points with a simple mount name prefix.

## Posix Locks

We want posix lock support with safe client eviction.

## Quotas

Quotas using FoundationDB commutative counters might be nice.

## Symlinks

Should be easy.

## Xattrs

Should be easy.

## Full test coverage

100 percent test coverage.

## Review code for TODO

There are various XXX and TODO in the code that need to be addressed or decided upon.