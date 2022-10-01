# TODO

There are things we need to address before we are production ready (if this meets our suitability criteria).

## Inode counter contention

We currently use a single key for inodes number generation - this is a huge point of contention.

## Permission checks

Permission checks are all done via fuse - however because HAFS is a distributed filesystem there
are still TOCTOU races - we need to do our own permission checks within transactions to be sure.

It should be noted these might not be worth fixing if the various cases are documented - For example
to prevent users affecting eachother they could simple deny other users any access to their directories at all.

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

## Quotas

Quotas using FoundationDB commutative counters might be nice.

## Respect relatime

How can we properly respect atime and relatime? Is it a fuse flag we must respect?

## Way to disable mtime on directories

This might help remove contention on huge directories. Maybe we want relmtime?

## Full test coverage

100 percent test coverage.

## Review code for TODO

There are various XXX and TODO in the code that need to be addressed or decided upon.

## Options for fuse dir and file cache timeout.

dent and file caching control is very important for a ditributed client.