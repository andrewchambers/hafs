# TODO

The filesystem works well for many use cases, but there are many
things that could still be improved.

## Permission checks

Permission checks are all done via fuse - however because HAFS is a distributed filesystem there
are still TOCTOU races - we need to do our own permission checks within transactions to resolve this.

It should be noted these might not be worth fixing if the various cases are documented - For example
to prevent users affecting eachother they could simple deny other users any access to their directories at all.

## Loop checks

In certain situations it is possible to move a directory inside itself and it will become unreachable,
if there is an efficient way to avoid this we could consider it - though it might not be worth fixing.

## Respect relatime

How can we properly respect atime and relatime? Is it a fuse flag we must respect?

## Full test coverage

100 percent test coverage.

## Review code for TODO

There are various XXX and TODO in the code that need to be addressed or decided upon.

