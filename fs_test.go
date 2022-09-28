package fs

import (
	"errors"
	"testing"

	"github.com/andrewchambers/foundation-fs/testutil"
)

func tmpFs(t *testing.T) *Fs {
	db := testutil.NewFDBTestServer(t).Dial()
	err := Mkfs(db)
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
	stat, err := fs.Stat(ROOT_INO)
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

func TestCreate(t *testing.T) {
	fs := tmpFs(t)

	ino, err := fs.Create(ROOT_INO, "foo", CreateOpts{
		Mode: S_IFDIR | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = fs.Create(ROOT_INO, "foo", CreateOpts{
		Mode: S_IFDIR | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if !errors.Is(err, ErrExist) {
		t.Fatal(err)
	}

	directStat, err := fs.Stat(ino)
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

	ino, err := fs.Create(ROOT_INO, "foo", CreateOpts{
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

	_, err = fs.Stat(ino)
	if !errors.Is(err, ErrNotExist) {
		t.Fatal(err)
	}
}

func TestRenameSameDir(t *testing.T) {
	fs := tmpFs(t)

	foo1Ino, err := fs.Create(ROOT_INO, "foo1", CreateOpts{
		Mode: S_IFDIR | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = fs.Create(ROOT_INO, "foo2", CreateOpts{
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

	rootStat, err := fs.Stat(ROOT_INO)
	if err != nil {
		t.Fatal(err)
	}
	if rootStat.Nchild != 2 {
		t.Fatalf("unexpected number of children: %d", rootStat.Nchild)
	}

}

func TestRenameSameDirOverwrite(t *testing.T) {
	fs := tmpFs(t)

	foo1Ino, err := fs.Create(ROOT_INO, "foo1", CreateOpts{
		Mode: S_IFDIR | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = fs.Create(ROOT_INO, "bar1", CreateOpts{
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

	rootStat, err := fs.Stat(ROOT_INO)
	if err != nil {
		t.Fatal(err)
	}
	if rootStat.Nchild != 1 {
		t.Fatalf("unexpected number of children: %d", rootStat.Nchild)
	}

}

func TestRenameDifferentDir(t *testing.T) {
	fs := tmpFs(t)

	dIno, err := fs.Create(ROOT_INO, "d", CreateOpts{
		Mode: S_IFDIR | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if err != nil {
		t.Fatal(err)
	}

	fooIno, err := fs.Create(ROOT_INO, "foo", CreateOpts{
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

	rootStat, err := fs.Stat(ROOT_INO)
	if err != nil {
		t.Fatal(err)
	}
	if rootStat.Nchild != 1 {
		t.Fatalf("unexpected number of children: %d", rootStat.Nchild)
	}

	dStat, err := fs.Stat(ROOT_INO)
	if err != nil {
		t.Fatal(err)
	}
	if dStat.Nchild != 1 {
		t.Fatalf("unexpected number of children: %d", dStat.Nchild)
	}

}

func TestRenameDifferentDirOverwrite(t *testing.T) {
	fs := tmpFs(t)

	dIno, err := fs.Create(ROOT_INO, "d", CreateOpts{
		Mode: S_IFDIR | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if err != nil {
		t.Fatal(err)
	}

	barIno, err := fs.Create(dIno, "bar", CreateOpts{
		Mode: S_IFREG | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if err != nil {
		t.Fatal(err)
	}

	fooIno, err := fs.Create(ROOT_INO, "foo", CreateOpts{
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

	rootStat, err := fs.Stat(ROOT_INO)
	if err != nil {
		t.Fatal(err)
	}
	if rootStat.Nchild != 1 {
		t.Fatalf("unexpected number of children: %d", rootStat.Nchild)
	}

	dStat, err := fs.Stat(dIno)
	if err != nil {
		t.Fatal(err)
	}
	if dStat.Nchild != 1 {
		t.Fatalf("unexpected number of children: %d", dStat.Nchild)
	}

	_, err = fs.Stat(barIno)
	if !errors.Is(err, ErrNotExist) {
		t.Fatal(err)
	}

}
