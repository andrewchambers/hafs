# High Availability Filesystem

An experimental and vapourware distributed filesystem library and fuse filesystem built on FoundationDB.

# Goals

## Motivation

[bupstash.io](https://bupstash.io/) needs a reliable and fault tolerant way to serve repository metadata 
that is easy to administer and distribute across many machines. This project is an experiment to investigate the feasibiltiy
and complexity of using FoundationDB as the base system for a metadata filesystem.

## Status

Basic functionality is in place and works against real programs.
See the [todos](./TODO.md) for what needs to be addressed.

## Initial Goals

- A distributed filesystem suitable for metadata.
- Support for efficient distributed posix whole file locks with safe client eviction.
- 100 percent test coverage.

## Stretch goals

- Optional directories that are efficient for bulk data (rados/s3/... backed files).
- Directly level tuning of stat and dent caching.
