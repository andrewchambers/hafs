# High Availability Filesystem

A distributed filesystem library and fuse filesystem built on FoundationDB meant
for scalable serving of metadata and file locks across a cluster.

You could think of HAFS like consul or etcd, but as a fuse filesystem - applications can use file locks and write + rename to do atomic updates across your entire cluster without any library support.

The HAFS filesystem also has the ability to store file data in s3 and other
external data to create horizontally scalable directories that work well
sequential read workloads.

## Motivation

My project [bupstash.io](https://bupstash.io/) needed a reliable and fault tolerant
way to serve repository metadata that is easy to administer and distribute across many machines.

## Features

- A distributed horizontal filesystem suitable for metadata and configuration across a cluster.
- Directories can efficiently scale to huge numbers of files.
- Optional directories/files that are efficient for bulk data (s3 backed files).
- Support for distributed posix whole file locks and BSD locks with client eviction - if
  a lock is broken, that client can no longer read or write to the filesystem without remounting.

## Current Limitations

- Posix locks do not support partial file range locks (non exclusive sqlite3 doesn't work yet).
- Files backed by s3 or other object storage are not efficient for random access, only sequential access.


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