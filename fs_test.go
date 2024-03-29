package hafs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	mathrand "math/rand"
	"os"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

func tmpFs(t *testing.T) *Fs {
	db := tmpDB(t)
	fs, err := Attach(db, "testfs", AttachOpts{})
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

func TestExclusiveLockRecordMarshalAndUnmarshal(t *testing.T) {
	t.Parallel()
	lr1 := exclusiveLockRecord{
		Owner:    0xffffffffffffffff,
		ClientId: "foobar",
	}
	lr2 := exclusiveLockRecord{}
	buf, _ := lr1.MarshalBinary()
	_ = lr2.UnmarshalBinary(buf)
	if lr1 != lr2 {
		t.Fatalf("%v != %v", lr1, lr2)
	}
}

func TestDirEntMarshalAndUnmarshal(t *testing.T) {
	t.Parallel()
	e1 := DirEnt{
		Mode: S_IFDIR,
		Ino:  12345,
	}
	e2 := DirEnt{}
	buf, _ := e1.MarshalBinary()
	_ = e2.UnmarshalBinary(buf)
	if e1 != e2 {
		t.Fatalf("%v != %v", e1, e2)
	}
}

func TestStatMarshalAndUnmarshal(t *testing.T) {
	t.Parallel()
	s1 := Stat{
		Size:      1,
		Atimesec:  2,
		Mtimesec:  3,
		Ctimesec:  4,
		Atimensec: 5,
		Mtimensec: 6,
		Ctimensec: 7,
		Mode:      8,
		Nlink:     9,
		Uid:       10,
		Gid:       11,
		Rdev:      12,
	}
	s2 := Stat{}
	buf, _ := s1.MarshalBinary()
	_ = s2.UnmarshalBinary(buf)
	if s1 != s2 {
		t.Fatalf("%v != %v", s1, s2)
	}
}

func TestMkfsAndAttach(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	fs := tmpFs(t)

	fooStat, err := fs.Mknod(ROOT_INO, "foo", MknodOpts{
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

	directStat, err := fs.GetStat(fooStat.Ino)
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

func TestSymlink(t *testing.T) {
	t.Parallel()
	fs := tmpFs(t)
	fooStat, err := fs.Mknod(ROOT_INO, "foo", MknodOpts{
		Mode:       S_IFLNK | 0o777,
		Uid:        0,
		Gid:        0,
		LinkTarget: []byte("abc"),
	})
	if err != nil {
		t.Fatal(err)
	}

	l, err := fs.ReadSymlink(fooStat.Ino)
	if err != nil {
		t.Fatal(err)
	}

	if string(l) != "abc" {
		t.Fatalf("unexpected link target: %v", l)
	}
}

func TestMknodTruncate(t *testing.T) {
	t.Parallel()
	fs := tmpFs(t)

	fooStat1, err := fs.Mknod(ROOT_INO, "foo", MknodOpts{
		Mode: S_IFREG | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if err != nil {
		t.Fatal(err)
	}

	fooStat2, err := fs.Mknod(ROOT_INO, "foo", MknodOpts{
		Truncate: true,
		Mode:     S_IFDIR | 0o777,
		Uid:      0,
		Gid:      0,
	})
	if err != nil {
		t.Fatal(err)
	}

	if fooStat1.Ino != fooStat2.Ino {
		t.Fatalf("inodes differ")
	}
}

func TestUnlink(t *testing.T) {
	t.Parallel()
	fs := tmpFs(t)

	err := fs.Unlink(ROOT_INO, "foo")
	if !errors.Is(err, ErrNotExist) {
		t.Fatal(err)
	}

	fooStat, err := fs.Mknod(ROOT_INO, "foo", MknodOpts{
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

	stat, err := fs.GetStat(fooStat.Ino)
	if err != nil {
		t.Fatal(err)
	}

	if stat.Nlink != 0 {
		t.Fatal("expected unlinked")
	}

	nRemoved, err := fs.RemoveExpiredUnlinked(RemoveExpiredUnlinkedOptions{
		RemovalDelay: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	if nRemoved != 0 {
		t.Fatal("expected nothing to be removed")
	}

	nRemoved, err = fs.RemoveExpiredUnlinked(RemoveExpiredUnlinkedOptions{
		RemovalDelay: 0,
	})
	if err != nil {
		t.Fatal(err)
	}

	if nRemoved != 1 {
		t.Fatal("expected file to be removed")
	}

	nRemoved, err = fs.RemoveExpiredUnlinked(RemoveExpiredUnlinkedOptions{
		RemovalDelay: 0,
	})
	if err != nil {
		t.Fatal(err)
	}

	if nRemoved != 0 {
		t.Fatal("expected nothing to be removed")
	}
}

func TestObjectStorageWriteOnce(t *testing.T) {
	t.Parallel()
	db := tmpDB(t)

	storageDir := t.TempDir()

	err := SetObjectStorageSpec(db, "testfs", "file://"+storageDir, SetObjectStorageSpecOpts{})
	if err != nil {
		t.Fatal(err)
	}

	fs, err := Attach(db, "testfs", AttachOpts{})
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	err = fs.SetXAttr(ROOT_INO, "hafs.object-storage", []byte("true"))
	if err != nil {
		t.Fatal(err)
	}

	f1, stat, err := fs.CreateFile(ROOT_INO, "f", CreateFileOpts{
		Mode: 0o777,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer f1.Close()

	_, err = f1.WriteData([]byte{1}, 0)
	if err != nil {
		t.Fatal(err)
	}

	err = f1.Fsync()
	if err != nil {
		t.Fatal(err)
	}

	f2, stat, err := fs.OpenFile(stat.Ino, OpenFileOpts{})
	if err != nil {
		t.Fatal(err)
	}
	defer f2.Close()

	_, err = f2.WriteData([]byte{1}, 0)
	if err == nil {
		t.Fatal(err)
	}

}

func TestObjectStorageReadWrite(t *testing.T) {
	t.Parallel()
	db := tmpDB(t)

	storageDir := t.TempDir()

	err := SetObjectStorageSpec(db, "testfs", "file://"+storageDir, SetObjectStorageSpecOpts{})
	if err != nil {
		t.Fatal(err)
	}

	smallObjectOptimizationThreshold := int64(1024)

	fs, err := Attach(db, "testfs", AttachOpts{
		SmallObjectOptimizationThreshold: uint64(smallObjectOptimizationThreshold),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	err = fs.SetXAttr(ROOT_INO, "hafs.object-storage", []byte("true"))
	if err != nil {
		t.Fatal(err)
	}

	for i, sz := range []int64{1, 2, 10, smallObjectOptimizationThreshold, smallObjectOptimizationThreshold * 2} {

		f, stat, err := fs.CreateFile(ROOT_INO, fmt.Sprintf("f%d", i), CreateFileOpts{
			Mode: 0o777,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()

		expectedBuffer := make([]byte, 0, sz)
		for j := int64(0); j < sz; j += 1 {
			expectedBuffer = append(expectedBuffer, byte(j%256))
		}

		_, err = f.WriteData(expectedBuffer, 0)
		if err != nil {
			t.Fatal(err)
		}

		err = f.Fsync()
		if err != nil {
			t.Fatal(err)
		}

		err = f.Close()
		if err != nil {
			t.Fatal(err)
		}

		f, stat, err = fs.OpenFile(stat.Ino, OpenFileOpts{})
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()

		buf := make([]byte, sz*2, sz*2)

		n, err := f.ReadData(buf, 0)
		if err != io.EOF {
			t.Fatal(err)
		}
		if int64(n) != sz {
			t.Fatal(n)
		}

		if !bytes.Equal(buf[:n], expectedBuffer) {
			t.Fatal(buf)
		}

		if sz < smallObjectOptimizationThreshold {
			_ = f.(*objectStoreSmallReadOnlyFile)
		} else {
			_ = f.(*objectStoreReadOnlyFile)
		}
	}

}

func TestObjectStorageUnlink(t *testing.T) {
	t.Parallel()
	db := tmpDB(t)

	storageDir := t.TempDir()

	err := SetObjectStorageSpec(db, "testfs", "file://"+storageDir, SetObjectStorageSpecOpts{})
	if err != nil {
		t.Fatal(err)
	}

	fs, err := Attach(db, "testfs", AttachOpts{})
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	err = fs.SetXAttr(ROOT_INO, "hafs.object-storage", []byte("true"))
	if err != nil {
		t.Fatal(err)
	}

	f, stat, err := fs.CreateFile(ROOT_INO, "f", CreateFileOpts{
		Mode: 0o777,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	_, err = f.WriteData([]byte{1}, 0)
	if err != nil {
		t.Fatal(err)
	}

	err = f.Fsync()
	if err != nil {
		t.Fatal(err)
	}

	inodeDataPath := fmt.Sprintf("%s/%016x.%s", storageDir, stat.Ino, fs.fsName)
	_, err = os.Stat(inodeDataPath)
	if err != nil {
		t.Fatal(err)
	}

	err = fs.Unlink(ROOT_INO, "f")
	if err != nil {
		t.Fatal(err)
	}

	nRemoved, err := fs.RemoveExpiredUnlinked(RemoveExpiredUnlinkedOptions{
		RemovalDelay: 0,
	})
	if err != nil {
		t.Fatal(err)
	}

	if nRemoved != 1 {
		t.Fatal("expected file to be removed")
	}

	_, err = os.Stat(inodeDataPath)
	if !errors.Is(err, iofs.ErrNotExist) {
		t.Fatal(err)
	}

}

func TestRenameSameDir(t *testing.T) {
	t.Parallel()
	fs := tmpFs(t)

	foo1Stat, err := fs.Mknod(ROOT_INO, "foo1", MknodOpts{
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

	if bar1Stat.Ino != foo1Stat.Ino {
		t.Fatalf("bar1 stat is bad: %#v", bar1Stat)
	}

}

func TestRenameSameDirOverwrite(t *testing.T) {
	t.Parallel()
	fs := tmpFs(t)

	foo1Stat, err := fs.Mknod(ROOT_INO, "foo1", MknodOpts{
		Mode: S_IFDIR | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if err != nil {
		t.Fatal(err)
	}

	f, _, err := fs.CreateFile(ROOT_INO, "bar1", CreateFileOpts{
		Mode: 0o777,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = f.WriteData([]byte{1, 2, 3, 4, 5}, 0)
	if err != nil {
		t.Fatal(err)
	}

	err = f.Close()
	if err != nil {
		t.Fatal(err)
	}

	bcnt, err := fs.SubvolumeByteCount(ROOT_INO)
	if err != nil {
		t.Fatal(err)
	}
	if bcnt != 5 {
		t.Fatal(bcnt)
	}

	icnt, err := fs.SubvolumeInodeCount(ROOT_INO)
	if err != nil {
		t.Fatal(err)
	}
	if icnt != 2 {
		t.Fatal(icnt)
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

	if bar1Stat.Ino != foo1Stat.Ino {
		t.Fatalf("bar1 stat is bad: %#v", bar1Stat)
	}

	nRemoved, err := fs.RemoveExpiredUnlinked(RemoveExpiredUnlinkedOptions{
		RemovalDelay: 0,
	})
	if err != nil {
		t.Fatal(err)
	}

	if nRemoved != 1 {
		t.Fatal("expected file to be removed")
	}

	bcnt, err = fs.SubvolumeByteCount(ROOT_INO)
	if err != nil {
		t.Fatal(err)
	}
	if bcnt != 0 {
		t.Fatal(bcnt)
	}

	icnt, err = fs.SubvolumeInodeCount(ROOT_INO)
	if err != nil {
		t.Fatal(err)
	}
	if icnt != 1 {
		t.Fatal(icnt)
	}

}

func TestRenameDifferentDir(t *testing.T) {
	t.Parallel()
	fs := tmpFs(t)

	dStat, err := fs.Mknod(ROOT_INO, "d", MknodOpts{
		Mode: S_IFDIR | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if err != nil {
		t.Fatal(err)
	}

	fooStat, err := fs.Mknod(ROOT_INO, "foo", MknodOpts{
		Mode: S_IFREG | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = fs.Rename(ROOT_INO, dStat.Ino, "foo", "bar")
	if err != nil {
		t.Fatal(err)
	}

	_, err = fs.Lookup(ROOT_INO, "foo")
	if !errors.Is(err, ErrNotExist) {
		t.Fatal(err)
	}

	barStat, err := fs.Lookup(dStat.Ino, "bar")
	if err != nil {
		t.Fatal(err)
	}

	if barStat.Ino != fooStat.Ino {
		t.Fatalf("bar1 stat is bad: %#v", barStat)
	}

}

func TestRenameDifferentDirOverwrite(t *testing.T) {
	t.Parallel()
	fs := tmpFs(t)

	dStat, err := fs.Mknod(ROOT_INO, "d", MknodOpts{
		Mode: S_IFDIR | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = fs.Mknod(dStat.Ino, "bar", MknodOpts{
		Mode: S_IFREG | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if err != nil {
		t.Fatal(err)
	}

	fooStat, err := fs.Mknod(ROOT_INO, "foo", MknodOpts{
		Mode: S_IFREG | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = fs.Rename(ROOT_INO, dStat.Ino, "foo", "bar")
	if err != nil {
		t.Fatal(err)
	}

	_, err = fs.Lookup(ROOT_INO, "foo")
	if !errors.Is(err, ErrNotExist) {
		t.Fatal(err)
	}

	barStat, err := fs.Lookup(dStat.Ino, "bar")
	if err != nil {
		t.Fatal(err)
	}

	if barStat.Ino != fooStat.Ino {
		t.Fatalf("bar1 stat is bad: %#v", barStat)
	}

	nRemoved, err := fs.RemoveExpiredUnlinked(RemoveExpiredUnlinkedOptions{
		RemovalDelay: 0,
	})
	if err != nil {
		t.Fatal(err)
	}

	if nRemoved != 1 {
		t.Fatal("expected file to be removed")
	}

}

func TestDirIter(t *testing.T) {
	t.Parallel()
	fs := tmpFs(t)

	const COUNT = _DIR_ITER_BATCH_SIZE*3 + _DIR_ITER_BATCH_SIZE/3

	for i := 0; i < COUNT; i += 1 {
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
		if err != nil {
			t.Fatalf("unexpected error %s", err)
		}
		count += 1
	}

	if count != COUNT {
		t.Fatalf("unexpected count: %d", count)
	}
}

func TestDirIterPlus(t *testing.T) {
	t.Parallel()
	fs := tmpFs(t)

	const COUNT = _DIR_ITER_BATCH_SIZE*2 + 57

	for i := 0; i < COUNT; i += 1 {
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
		dirEnt, stat, err := di.NextPlus()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("unexpected error %s", err)
		}
		if dirEnt.Ino != stat.Ino {
			t.Fatalf("stat unexpectedly differs from dir ent: %#v %#v", dirEnt, stat)
		}
		count += 1
	}

	if count != COUNT {
		t.Fatalf("unexpected count: %d", count)
	}
}

func TestWriteDataOneChunk(t *testing.T) {
	t.Parallel()
	fs := tmpFs(t)

	testSizes := []uint64{0, 1, 3, CHUNK_SIZE - 1, CHUNK_SIZE}

	for i, n := range testSizes {
		name := fmt.Sprintf("d%d", i)
		f, stat, err := fs.CreateFile(ROOT_INO, name, CreateFileOpts{
			Mode: 0o777,
		})
		if err != nil {
			t.Fatal(err)
		}

		data := make([]byte, n, n)

		for j := 0; j < len(data); j += 1 {
			data[j] = byte(j % 256)
		}

		nWritten, err := f.WriteData(data, 0)
		if err != nil {
			t.Fatal(err)
		}
		if nWritten != uint32(len(data)) {
			t.Fatalf("unexpected write amount %d != %d", nWritten, len(data))
		}

		fetchedData, err := fs.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
			data := tx.Get(tuple.Tuple{"hafs", fs.fsName, "ino", stat.Ino, "data", 0}).MustGet()
			zeroExpandChunk(&data)
			return data, nil
		})
		if err != nil {
			t.Fatal(err)
		}

		if len(fetchedData.([]byte)) != CHUNK_SIZE {
			t.Fatalf("unexpected chunk size: %d", fetchedData)
		}

		if !bytes.Equal(data, fetchedData.([]byte)[:len(data)]) {
			t.Fatalf("%v != %v", data, fetchedData)
		}

	}
}

func TestWriteDataTwoChunks(t *testing.T) {
	t.Parallel()
	fs := tmpFs(t)

	testSizes := []uint64{CHUNK_SIZE + 1, CHUNK_SIZE + 3, CHUNK_SIZE * 2}

	for i, n := range testSizes {
		name := fmt.Sprintf("d%d", i)
		f, stat, err := fs.CreateFile(ROOT_INO, name, CreateFileOpts{
			Mode: 0o777,
		})
		if err != nil {
			t.Fatal(err)
		}

		data := make([]byte, n, n)

		for j := 0; j < len(data); j += 1 {
			data[j] = byte(j % 256)
		}

		data1 := data[:CHUNK_SIZE]
		data2 := data[CHUNK_SIZE:]

		nWritten := 0
		for nWritten != len(data) {
			n, err := f.WriteData(data[nWritten:], uint64(nWritten))
			if err != nil {
				t.Fatal(err)
			}
			nWritten += int(n)
		}

		fetchedData1, err := fs.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
			data := tx.Get(tuple.Tuple{"hafs", fs.fsName, "ino", stat.Ino, "data", 0}).MustGet()
			zeroExpandChunk(&data)
			return data, nil
		})
		if err != nil {
			t.Fatal(err)
		}

		fetchedData2, err := fs.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
			data := tx.Get(tuple.Tuple{"hafs", fs.fsName, "ino", stat.Ino, "data", 1}).MustGet()
			zeroExpandChunk(&data)
			return data, nil
		})
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(data1, fetchedData1.([]byte)[:len(data1)]) {
			t.Fatalf("%v != %v", data, fetchedData1)
		}

		if !bytes.Equal(data2, fetchedData2.([]byte)[:len(data2)]) {
			t.Fatalf("%v != %v", data, fetchedData2)
		}

		stat, err = fs.GetStat(stat.Ino)
		if err != nil {
			t.Fatal(err)
		}

		if stat.Size != uint64(len(data)) {
			t.Fatalf("unexpected size - %d != %d", stat.Size, len(data))
		}

	}
}

func TestTruncate(t *testing.T) {
	t.Parallel()
	fs := tmpFs(t)

	testSizes := []uint64{
		CHUNK_SIZE - 1,
		CHUNK_SIZE + 1,
		CHUNK_SIZE*2 - 1,
		CHUNK_SIZE * 2,
		CHUNK_SIZE*2 + 1,
	}

	for i, n := range testSizes {
		name := fmt.Sprintf("d%d", i)
		f, stat, err := fs.CreateFile(ROOT_INO, name, CreateFileOpts{
			Mode: 0o777,
			Uid:  0,
			Gid:  0,
		})
		if err != nil {
			t.Fatal(err)
		}

		data := make([]byte, n, n)

		for j := 0; j < len(data); j += 1 {
			data[j] = byte(j % 256)
		}

		nWritten := 0
		for nWritten != len(data) {
			n, err := f.WriteData(data[nWritten:], uint64(nWritten))
			if err != nil {
				t.Fatal(err)
			}
			nWritten += int(n)
		}

		_, err = fs.ModStat(stat.Ino, ModStatOpts{
			Valid: MODSTAT_SIZE,
			Size:  5,
		})
		if err != nil {
			t.Fatal(err)
		}

		_, err = fs.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
			kvs := tx.GetRange(tuple.Tuple{"hafs", fs.fsName, "ino", stat.Ino, "data"}, fdb.RangeOptions{}).GetSliceOrPanic()
			if len(kvs) != 1 {
				t.Fatalf("bad number of data chunks: %d", len(kvs))
			}
			data := tx.Get(tuple.Tuple{"hafs", fs.fsName, "ino", stat.Ino, "data", 0}).MustGet()
			zeroExpandChunk(&data)
			if len(data) != CHUNK_SIZE {
				t.Fatalf("bad data size: %d", len(data))
			}
			if !bytes.Equal(data[:5], []byte{0, 1, 2, 3, 4}) {
				t.Fatalf("bad data: %v", data)
			}
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}

	}
}

func TestReadWriteData(t *testing.T) {
	t.Parallel()
	fs := tmpFs(t)

	// Random writes at different offsets to exercise the sparse code paths.
	for i := 0; i < 100; i++ {
		f, stat, err := fs.CreateFile(ROOT_INO, "f", CreateFileOpts{
			Mode: 0o777,
		})
		if err != nil {
			t.Fatal(err)
		}

		referenceFile, err := os.CreateTemp("", "")
		if err != nil {
			t.Fatal(err)
		}
		size := mathrand.Int()%(CHUNK_SIZE*3) + CHUNK_SIZE/2
		nwrites := mathrand.Int() % 5
		for i := 0; i < nwrites; i++ {
			writeOffset := mathrand.Int() % size
			writeSize := mathrand.Int() % (size - writeOffset)
			writeData := make([]byte, writeSize, writeSize)
			n, err := mathrand.Read(writeData)
			if err != nil || n != len(writeData) {
				t.Fatalf("%s %d", err, n)
			}
			n, err = referenceFile.WriteAt(writeData, int64(writeOffset))
			if err != nil || n != len(writeData) {
				t.Fatalf("%s %d", err, n)
			}
			nWritten := 0
			for nWritten != len(writeData) {
				n, err := f.WriteData(writeData[nWritten:], uint64(writeOffset)+uint64(nWritten))
				if err != nil {
					t.Fatal(err)
				}
				nWritten += int(n)
			}
		}

		referenceData, err := io.ReadAll(referenceFile)
		if err != nil {
			t.Fatal(err)
		}

		stat, err = fs.Lookup(ROOT_INO, "f")
		if err != nil {
			t.Fatal(err)
		}

		if stat.Size != uint64(len(referenceData)) {
			t.Fatalf("read lengths differ:\n%v\n!=%v\n", stat.Size, len(referenceData))
		}

		actualData := &bytes.Buffer{}
		nRead := uint64(0)
		readSize := (mathrand.Int() % 2 * CHUNK_SIZE) + 100
		readBuf := make([]byte, readSize, readSize)
		for {
			n, err := f.ReadData(readBuf, nRead)
			nRead += uint64(n)
			_, _ = actualData.Write(readBuf[:n])
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if nRead > uint64(len(referenceData)) {
				t.Fatalf("file too large - expected %d bytes, but read %d ", len(referenceData), nRead)
			}
		}

		if len(referenceData) != actualData.Len() {
			t.Fatalf("read lengths differ:\n%v\n!=%v\n", len(referenceData), actualData.Len())
		}

		if !bytes.Equal(referenceData, actualData.Bytes()) {
			t.Fatalf("read corrupt:\n%v\n!=%v\n", referenceData, actualData.Bytes())
		}

		_ = referenceFile.Close()

		err = fs.Unlink(ROOT_INO, "f")
		if err != nil {
			t.Fatal(err)
		}
	}

}

func TestSetLock(t *testing.T) {
	t.Parallel()
	fs := tmpFs(t)

	stat, err := fs.Mknod(ROOT_INO, "f", MknodOpts{
		Mode: S_IFREG | 0o777,
		Uid:  0,
		Gid:  0,
	})

	ok, err := fs.TrySetLock(stat.Ino, SetLockOpts{
		Typ:   LOCK_NONE,
		Owner: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal()
	}

	ok, err = fs.TrySetLock(stat.Ino, SetLockOpts{
		Typ:   LOCK_SHARED,
		Owner: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal()
	}

	ok, err = fs.TrySetLock(stat.Ino, SetLockOpts{
		Typ:   LOCK_SHARED,
		Owner: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal()
	}

	ok, err = fs.TrySetLock(stat.Ino, SetLockOpts{
		Typ:   LOCK_EXCLUSIVE,
		Owner: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal()
	}

	ok, err = fs.TrySetLock(stat.Ino, SetLockOpts{
		Typ:   LOCK_NONE,
		Owner: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal()
	}

	ok, err = fs.TrySetLock(stat.Ino, SetLockOpts{
		Typ:   LOCK_NONE,
		Owner: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal()
	}

	ok, err = fs.TrySetLock(stat.Ino, SetLockOpts{
		Typ:   LOCK_EXCLUSIVE,
		Owner: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal()
	}

	ok, err = fs.TrySetLock(stat.Ino, SetLockOpts{
		Typ:   LOCK_EXCLUSIVE,
		Owner: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal()
	}

}

func TestWaitForLockWithWatch(t *testing.T) {
	t.Parallel()
	fs := tmpFs(t)

	stat, err := fs.Mknod(ROOT_INO, "f", MknodOpts{
		Mode: S_IFREG | 0o777,
		Uid:  0,
		Gid:  0,
	})

	ok, err := fs.TrySetLock(stat.Ino, SetLockOpts{
		Typ:   LOCK_EXCLUSIVE,
		Owner: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal()
	}

	waitStart := time.Now()

	go func() {
		time.Sleep(100 * time.Millisecond)
		ok, err := fs.TrySetLock(stat.Ino, SetLockOpts{
			Typ:   LOCK_NONE,
			Owner: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatal()
		}
	}()

	err = fs.AwaitExclusiveLockRelease(make(chan struct{}, 1), stat.Ino)
	if err != nil {
		t.Fatal(err)
	}

	ok, err = fs.TrySetLock(stat.Ino, SetLockOpts{
		Typ:   LOCK_EXCLUSIVE,
		Owner: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal()
	}

	if time.Since(waitStart) > 200*time.Millisecond {
		t.Fatal("wait took too long")
	}
}

func TestWaitForLockWithPoll(t *testing.T) {
	t.Parallel()
	fs := tmpFs(t)

	stat, err := fs.Mknod(ROOT_INO, "f", MknodOpts{
		Mode: S_IFREG | 0o777,
		Uid:  0,
		Gid:  0,
	})

	ok, err := fs.TrySetLock(stat.Ino, SetLockOpts{
		Typ:   LOCK_EXCLUSIVE,
		Owner: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal()
	}

	waitStart := time.Now()

	go func() {
		time.Sleep(100 * time.Millisecond)
		ok, err := fs.TrySetLock(stat.Ino, SetLockOpts{
			Typ:   LOCK_NONE,
			Owner: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatal()
		}
	}()

	err = fs.PollAwaitExclusiveLockRelease(make(chan struct{}, 1), stat.Ino)
	if err != nil {
		t.Fatal(err)
	}

	ok, err = fs.TrySetLock(stat.Ino, SetLockOpts{
		Typ:   LOCK_EXCLUSIVE,
		Owner: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal()
	}

	if time.Since(waitStart) > 1200*time.Millisecond {
		t.Fatal("wait took too long")
	}
}

func TestInodeAllocation(t *testing.T) {
	t.Parallel()
	fs := tmpFs(t)
	seen := make(map[uint64]struct{})
	for i := uint64(0); i < _INO_STEP*10; i++ {
		ino, err := fs.nextIno()
		if err != nil {
			t.Fatal(err)
		}
		//t.Logf("%016x %016x", ino, bits.Reverse64(ino))
		_, seenBefore := seen[ino]
		if seenBefore {
			t.Fatal("repeated inode")
		}
		seen[ino] = struct{}{}
	}
}

func TestHardLink(t *testing.T) {
	t.Parallel()
	fs := tmpFs(t)

	foo1Stat, err := fs.Mknod(ROOT_INO, "foo1", MknodOpts{
		Mode: S_IFREG | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if err != nil {
		t.Fatal(err)
	}

	foo2Stat, err := fs.HardLink(ROOT_INO, foo1Stat.Ino, "foo2")
	if err != nil {
		t.Fatal(err)
	}

	if foo2Stat.Nlink != 2 {
		t.Fatal(err)
	}

	foo1Stat, err = fs.Lookup(ROOT_INO, "foo1")
	if err != nil {
		t.Fatal(err)
	}
	if foo1Stat.Nlink != 2 {
		t.Fatal(err)
	}

	foo1Stat, err = fs.Lookup(ROOT_INO, "foo1")
	if err != nil {
		t.Fatal(err)
	}
	if foo1Stat.Nlink != 2 {
		t.Fatal(err)
	}

	if foo1Stat.Ino != foo2Stat.Ino {
		t.Fatal("inos differ")
	}

	err = fs.Unlink(ROOT_INO, "foo2")
	if err != nil {
		t.Fatal(err)
	}

	foo1Stat, err = fs.Lookup(ROOT_INO, "foo1")
	if err != nil {
		t.Fatal(err)
	}
	if foo1Stat.Nlink != 1 {
		t.Fatal(err)
	}

	nRemoved, err := fs.RemoveExpiredUnlinked(RemoveExpiredUnlinkedOptions{
		RemovalDelay: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if nRemoved != 0 {
		t.Fatal("unexpected remove count")
	}

	err = fs.Unlink(ROOT_INO, "foo1")
	if err != nil {
		t.Fatal(err)
	}

	nRemoved, err = fs.RemoveExpiredUnlinked(RemoveExpiredUnlinkedOptions{
		RemovalDelay: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if nRemoved != 1 {
		t.Fatal("unexpected remove count")
	}

}

func TestHardLinkDirFails(t *testing.T) {
	t.Parallel()
	fs := tmpFs(t)

	foo1Stat, err := fs.Mknod(ROOT_INO, "foo1", MknodOpts{
		Mode: S_IFDIR | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = fs.HardLink(ROOT_INO, foo1Stat.Ino, "foo2")
	if err != ErrPermission {
		t.Fatal(err)
	}

}

func TestClientSelfEvictExclusiveLock(t *testing.T) {
	t.Parallel()
	db := tmpDB(t)
	fs1, err := Attach(db, "testfs", AttachOpts{})
	if err != nil {
		t.Fatal(err)
	}
	defer fs1.Close()

	fs2, err := Attach(db, "testfs", AttachOpts{})
	if err != nil {
		t.Fatal(err)
	}
	defer fs2.Close()

	stat, err := fs1.Mknod(ROOT_INO, "f", MknodOpts{
		Mode: S_IFREG | 0o777,
		Uid:  0,
		Gid:  0,
	})

	ok, err := fs1.TrySetLock(stat.Ino, SetLockOpts{
		Typ:   LOCK_EXCLUSIVE,
		Owner: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal()
	}

	err = fs1.Close()
	if err != nil {
		t.Fatal(err)
	}

	ok, err = fs2.TrySetLock(stat.Ino, SetLockOpts{
		Typ:   LOCK_EXCLUSIVE,
		Owner: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal()
	}

}

func TestClientSelfEvictSharedLock(t *testing.T) {
	t.Parallel()
	db := tmpDB(t)
	fs1, err := Attach(db, "testfs", AttachOpts{})
	if err != nil {
		t.Fatal(err)
	}
	defer fs1.Close()

	fs2, err := Attach(db, "testfs", AttachOpts{})
	if err != nil {
		t.Fatal(err)
	}
	defer fs2.Close()

	stat, err := fs1.Mknod(ROOT_INO, "f", MknodOpts{
		Mode: S_IFREG | 0o777,
		Uid:  0,
		Gid:  0,
	})

	ok, err := fs1.TrySetLock(stat.Ino, SetLockOpts{
		Typ:   LOCK_SHARED,
		Owner: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal()
	}

	err = fs1.Close()
	if err != nil {
		t.Fatal(err)
	}

	ok, err = fs2.TrySetLock(stat.Ino, SetLockOpts{
		Typ:   LOCK_EXCLUSIVE,
		Owner: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal()
	}

}

func TestSubvolume(t *testing.T) {
	t.Parallel()
	fs := tmpFs(t)

	dir, err := fs.Mknod(ROOT_INO, "d", MknodOpts{
		Mode: S_IFDIR | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if err != nil {
		t.Fatal(err)
	}

	f, err := fs.Mknod(dir.Ino, "f", MknodOpts{
		Mode: S_IFREG | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if f.Subvolume != ROOT_INO {
		t.Fatal("unexpected subvolume")
	}

	err = fs.SetXAttr(dir.Ino, "hafs.subvolume", []byte("true"))
	if err != ErrInvalid {
		t.Fatal(err)
	}

	err = fs.Unlink(dir.Ino, "f")
	if err != nil {
		t.Fatal(err)
	}

	err = fs.SetXAttr(dir.Ino, "hafs.subvolume", []byte("true"))
	if err != nil {
		t.Fatal(err)
	}

	dir, err = fs.GetStat(dir.Ino)
	if err != nil {
		t.Fatal(err)
	}

	if dir.Flags&FLAG_SUBVOLUME != FLAG_SUBVOLUME {
		t.Fatal("expected subvolume flag to be set")
	}

	f, err = fs.Mknod(dir.Ino, "f", MknodOpts{
		Mode: S_IFREG | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if f.Subvolume != dir.Ino {
		t.Fatal("unexpected subvolume")
	}

	err = fs.RemoveXAttr(dir.Ino, "hafs.subvolume")
	if err == nil {
		t.Fatal(err)
	}

	err = fs.RemoveXAttr(dir.Ino, "hafs.subvolume")
	if err != ErrInvalid {
		t.Fatal(err)
	}

	err = fs.Unlink(dir.Ino, "f")
	if err != nil {
		t.Fatal(err)
	}

	err = fs.RemoveXAttr(dir.Ino, "hafs.subvolume")
	if err != nil {
		t.Fatal(err)
	}

	dir, err = fs.GetStat(dir.Ino)
	if err != nil {
		t.Fatal(err)
	}
	if dir.Flags&FLAG_SUBVOLUME != 0 {
		t.Fatal("expected subvolume flag to be unset")
	}

}

func TestSubvolumeNoCrossRenames(t *testing.T) {
	t.Parallel()
	fs := tmpFs(t)

	dir, err := fs.Mknod(ROOT_INO, "d", MknodOpts{
		Mode: S_IFDIR | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = fs.SetXAttr(dir.Ino, "hafs.subvolume", []byte("true"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = fs.Mknod(dir.Ino, "f", MknodOpts{
		Mode: S_IFREG | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = fs.Rename(dir.Ino, ROOT_INO, "f", "f")
	if err != ErrInvalid {
		t.Fatal(err)
	}

}

func TestSubvolumeNoCrossHardlinks(t *testing.T) {
	t.Parallel()
	fs := tmpFs(t)

	dir, err := fs.Mknod(ROOT_INO, "d", MknodOpts{
		Mode: S_IFDIR | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = fs.SetXAttr(dir.Ino, "hafs.subvolume", []byte("true"))
	if err != nil {
		t.Fatal(err)
	}

	f, err := fs.Mknod(dir.Ino, "f", MknodOpts{
		Mode: S_IFREG | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = fs.HardLink(ROOT_INO, f.Ino, "f")
	if err != ErrInvalid {
		t.Fatal(err)
	}

}

func TestSubvolumeByteAccounting(t *testing.T) {
	t.Parallel()
	fs := tmpFs(t)

	b, err := fs.SubvolumeByteCount(ROOT_INO)
	if err != nil {
		t.Fatal(err)
	}
	if b != 0 {
		t.Fatal(b)
	}

	f, fstat, err := fs.CreateFile(ROOT_INO, "f", CreateFileOpts{
		Mode: 0o777,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = f.WriteData([]byte{1, 2, 3, 4, 5}, 0)
	if err != nil {
		t.Fatal(err)
	}

	err = f.Close()
	if err != nil {
		t.Fatal(err)
	}

	b, err = fs.SubvolumeByteCount(ROOT_INO)
	if err != nil {
		t.Fatal(err)
	}
	if b != 5 {
		t.Fatal(b)
	}

	usageXattr, err := fs.GetXAttr(ROOT_INO, "hafs.total-bytes")
	if err != nil {
		t.Fatal(err)
	}
	if string(usageXattr) != "5" {
		t.Fatalf("unexpected usage xattr: %q", string(usageXattr))
	}

	fs.ModStat(fstat.Ino, ModStatOpts{
		Valid: MODSTAT_SIZE,
		Size:  3,
	})

	b, err = fs.SubvolumeByteCount(ROOT_INO)
	if err != nil {
		t.Fatal(err)
	}
	if b != 3 {
		t.Fatal(b)
	}

	err = fs.Unlink(ROOT_INO, "f")
	if err != nil {
		t.Fatal(err)
	}

	b, err = fs.SubvolumeByteCount(ROOT_INO)
	if err != nil {
		t.Fatal(err)
	}
	if b != 0 {
		t.Fatal(b)
	}

}
