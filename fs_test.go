package fs

import (
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/andrewchambers/foundation-fs/testutil"
)

func tmpFs(t *testing.T) *Fs {
	db := testutil.NewFDBTestServer(t).Dial()
	err := Mkfs(db, MkfsOpts{Overwrite: false})
	if err != nil {
		t.Fatal(err)
	}
	fs, err := Mount(db)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		err = fs.Close()
		if err != nil {
			t.Logf("unable to close fs: %s", err)
		}
	})
	return fs
}

func TestMkfsAndMount(t *testing.T) {
	fs := tmpFs(t)
	stat, err := fs.GetStat(ROOT_INO)
	if err != nil {
		t.Fatal(err)
	}
	if stat.Mode&S_IFMT != S_IFDIR {
		t.Fatal("unexpected mode")
	}
	err = fs.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestMknod(t *testing.T) {
	fs := tmpFs(t)

	ino, err := fs.Mknod(ROOT_INO, "foo", MknodOpts{
		Mode: S_IFDIR | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = fs.Mknod(ROOT_INO, "foo", MknodOpts{
		Mode: S_IFDIR | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if !errors.Is(err, ErrExist) {
		t.Fatal(err)
	}

	directStat, err := fs.GetStat(ino)
	if err != nil {
		t.Fatal(err)
	}

	lookupStat, err := fs.Lookup(ROOT_INO, "foo")
	if err != nil {
		t.Fatal(err)
	}

	if directStat != lookupStat {
		t.Fatalf("stats differ: %v != %v", directStat, lookupStat)
	}

	if directStat.Mode != S_IFDIR|0o777 {
		t.Fatalf("unexpected mode: %d", directStat.Mode)
	}

}

func TestUnlink(t *testing.T) {
	fs := tmpFs(t)

	err := fs.Unlink(ROOT_INO, "foo")
	if !errors.Is(err, ErrNotExist) {
		t.Fatal(err)
	}

	ino, err := fs.Mknod(ROOT_INO, "foo", MknodOpts{
		Mode: S_IFDIR | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = fs.Unlink(ROOT_INO, "foo")
	if err != nil {
		t.Fatal(err)
	}

	_, err = fs.GetStat(ino)
	if !errors.Is(err, ErrNotExist) {
		t.Fatal(err)
	}
}

func TestRenameSameDir(t *testing.T) {
	fs := tmpFs(t)

	foo1Ino, err := fs.Mknod(ROOT_INO, "foo1", MknodOpts{
		Mode: S_IFDIR | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = fs.Mknod(ROOT_INO, "foo2", MknodOpts{
		Mode: S_IFREG | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = fs.Rename(ROOT_INO, ROOT_INO, "foo1", "bar1")
	if err != nil {
		t.Fatal(err)
	}

	_, err = fs.Lookup(ROOT_INO, "foo1")
	if !errors.Is(err, ErrNotExist) {
		t.Fatal(err)
	}

	bar1Stat, err := fs.Lookup(ROOT_INO, "bar1")
	if err != nil {
		t.Fatal(err)
	}

	if bar1Stat.Ino != foo1Ino {
		t.Fatalf("bar1 stat is bad: %#v", bar1Stat)
	}

	rootStat, err := fs.GetStat(ROOT_INO)
	if err != nil {
		t.Fatal(err)
	}
	if rootStat.Nchild != 2 {
		t.Fatalf("unexpected number of children: %d", rootStat.Nchild)
	}

}

func TestRenameSameDirOverwrite(t *testing.T) {
	fs := tmpFs(t)

	foo1Ino, err := fs.Mknod(ROOT_INO, "foo1", MknodOpts{
		Mode: S_IFDIR | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = fs.Mknod(ROOT_INO, "bar1", MknodOpts{
		Mode: S_IFREG | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = fs.Rename(ROOT_INO, ROOT_INO, "foo1", "bar1")
	if err != nil {
		t.Fatal(err)
	}

	_, err = fs.Lookup(ROOT_INO, "foo1")
	if !errors.Is(err, ErrNotExist) {
		t.Fatal(err)
	}

	bar1Stat, err := fs.Lookup(ROOT_INO, "bar1")
	if err != nil {
		t.Fatal(err)
	}

	if bar1Stat.Ino != foo1Ino {
		t.Fatalf("bar1 stat is bad: %#v", bar1Stat)
	}

	rootStat, err := fs.GetStat(ROOT_INO)
	if err != nil {
		t.Fatal(err)
	}
	if rootStat.Nchild != 1 {
		t.Fatalf("unexpected number of children: %d", rootStat.Nchild)
	}

}

func TestRenameDifferentDir(t *testing.T) {
	fs := tmpFs(t)

	dIno, err := fs.Mknod(ROOT_INO, "d", MknodOpts{
		Mode: S_IFDIR | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if err != nil {
		t.Fatal(err)
	}

	fooIno, err := fs.Mknod(ROOT_INO, "foo", MknodOpts{
		Mode: S_IFREG | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = fs.Rename(ROOT_INO, dIno, "foo", "bar")
	if err != nil {
		t.Fatal(err)
	}

	_, err = fs.Lookup(ROOT_INO, "foo")
	if !errors.Is(err, ErrNotExist) {
		t.Fatal(err)
	}

	barStat, err := fs.Lookup(dIno, "bar")
	if err != nil {
		t.Fatal(err)
	}

	if barStat.Ino != fooIno {
		t.Fatalf("bar1 stat is bad: %#v", barStat)
	}

	rootStat, err := fs.GetStat(ROOT_INO)
	if err != nil {
		t.Fatal(err)
	}
	if rootStat.Nchild != 1 {
		t.Fatalf("unexpected number of children: %d", rootStat.Nchild)
	}

	dStat, err := fs.GetStat(ROOT_INO)
	if err != nil {
		t.Fatal(err)
	}
	if dStat.Nchild != 1 {
		t.Fatalf("unexpected number of children: %d", dStat.Nchild)
	}

}

func TestRenameDifferentDirOverwrite(t *testing.T) {
	fs := tmpFs(t)

	dIno, err := fs.Mknod(ROOT_INO, "d", MknodOpts{
		Mode: S_IFDIR | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if err != nil {
		t.Fatal(err)
	}

	barIno, err := fs.Mknod(dIno, "bar", MknodOpts{
		Mode: S_IFREG | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if err != nil {
		t.Fatal(err)
	}

	fooIno, err := fs.Mknod(ROOT_INO, "foo", MknodOpts{
		Mode: S_IFREG | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = fs.Rename(ROOT_INO, dIno, "foo", "bar")
	if err != nil {
		t.Fatal(err)
	}

	_, err = fs.Lookup(ROOT_INO, "foo")
	if !errors.Is(err, ErrNotExist) {
		t.Fatal(err)
	}

	barStat, err := fs.Lookup(dIno, "bar")
	if err != nil {
		t.Fatal(err)
	}

	if barStat.Ino != fooIno {
		t.Fatalf("bar1 stat is bad: %#v", barStat)
	}

	rootStat, err := fs.GetStat(ROOT_INO)
	if err != nil {
		t.Fatal(err)
	}
	if rootStat.Nchild != 1 {
		t.Fatalf("unexpected number of children: %d", rootStat.Nchild)
	}

	dStat, err := fs.GetStat(dIno)
	if err != nil {
		t.Fatal(err)
	}
	if dStat.Nchild != 1 {
		t.Fatalf("unexpected number of children: %d", dStat.Nchild)
	}

	_, err = fs.GetStat(barIno)
	if !errors.Is(err, ErrNotExist) {
		t.Fatal(err)
	}

}

func TestDirIter(t *testing.T) {
	fs := tmpFs(t)

	for i := 0; i < 500; i += 1 {
		_, err := fs.Mknod(ROOT_INO, fmt.Sprintf("a%d", i), MknodOpts{
			Mode: S_IFDIR | 0o777,
			Uid:  0,
			Gid:  0,
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	di, err := fs.IterDirEnts(ROOT_INO)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for {
		_, err := di.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		count += 1
	}

	if count != 500 {
		t.Fatalf("unexpected count: %d", count)
	}
}
