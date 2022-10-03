package hafs

import (
	iofs "io/fs"
	"os"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"
)

func TestErrToFuseStatus(t *testing.T) {

	testCases := []struct {
		e error
		s fuse.Status
	}{
		{ErrNotExist, fuse.Status(unix.ENOENT)},
		{ErrExist, fuse.Status(unix.EEXIST)},
		{ErrNotEmpty, fuse.Status(unix.ENOTEMPTY)},
		{ErrNotDir, fuse.Status(unix.ENOTDIR)},
		{ErrInvalid, fuse.Status(unix.EINVAL)},
		{ErrPermission, fuse.Status(unix.EPERM)},
		{ErrIntr, fuse.Status(unix.EINTR)},

		{iofs.ErrPermission, fuse.Status(unix.EPERM)},
		{iofs.ErrNotExist, fuse.Status(unix.ENOENT)},
		{iofs.ErrExist, fuse.Status(unix.EEXIST)},
		{iofs.ErrInvalid, fuse.Status(unix.EINVAL)},

		{os.ErrPermission, fuse.Status(unix.EPERM)},
		{os.ErrNotExist, fuse.Status(unix.ENOENT)},
		{os.ErrExist, fuse.Status(unix.EEXIST)},
		{os.ErrInvalid, fuse.Status(unix.EINVAL)},
	}

	for _, tc := range testCases {
		if errToFuseStatus(tc.e) != tc.s {
			t.Fatalf("%v != %v", tc.e, tc.s)
		}
	}

}
