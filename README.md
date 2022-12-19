# High Availability Filesystem

A distributed filesystem library and fuse filesystem built on FoundationDB meant
for storing metadata and doing file locking across a cluster of servers or containers.

You could think of HAFS like consul or etcd, but as a fuse filesystem - applications can use file locks and write + rename to do atomic updates across your entire cluster without any library support.

The HAFS filesystem also has the ability to store file data in s3 and other
external data to create horizontally scalable directories that work well
sequential read and write workloads.

## Why does this exist

My project [bupstash.io](https://bupstash.io/) needed a reliable and fault tolerant
way to serve repository metadata across many machines. Having been bitten by NFS in the past, I wanted to be confident in all
the failure modes I would encounter when using distributed file locks.

### Ideal use cases

In general this filesystem is good for configuration files and metadata that must be kept consistent
across a whole cluster while also tolerating server failure, it is not great at handling large volumes
of data or having high write throughput (unless you use the s3 backed file feature).

## Features

- A distributed and consistent filesystem suitable for metadata and configuration across a cluster.
- Directories that can efficiently scale to huge numbers of files.
- Optional s3 backed files that are efficient for sequential access bulk data.
- Support for distributed posix whole file locks and BSD locks with client eviction - if
  a lock is broken, that client can no longer read or write to the filesystem without remounting.

## Current Limitations

- Posix locks do not support partial file range locks (so non exclusive sqlite3 doesn't work yet).
- Files backed by s3 or other object storage are not efficient for random access, only sequential access.
- Because HAFS is built on top of foundationdb, it requires at least 4GB of ram per node, which can be a bit heavy depending on the use case and project budget (though it works great on an existing foundationDB deployment).

## Caveats and Gotchas.

- File locking is only effective at protecting updates to the filesystem, you should not coordinate
  updates to resources outside the filesystem.

## Getting started

You will need:

- A running foundationdb cluster.
- The hafs binaries added to your PATH.

Create and mount a filesystem:

```
# Create the default 'fs' filesystem.
$ hafs-mkfs
$ hafs-fuse /mnt/hafs
```

From another terminal you can access hafs:

```
$ hafs-list-clients
...
$ ls /mnt/hafs
$ echo foo > /mnt/hafs/test.txt
```

From any number of other machines mount the same filesystem and have a fully consistent distributed
filesystem - including file locks.