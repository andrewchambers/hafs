package hafs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	mathrand "math/rand"
	"os"
	"testing"
	"time"

	"github.com/andrewchambers/hafs/testutil"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

func tmpFs(t *testing.T) *Fs {
	db := testutil.NewFDBTestServer(t).Dial()
	err := Mkfs(db, MkfsOpts{Overwrite: false})
	if err != nil {
		t.Fatal(err)
	}
	fs, err := Attach(db)
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

func TestMkfsAndAttach(t *testing.T) {
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

	nRemoved, err := fs.RemoveExpiredUnlinked(time.Duration(0))
	if err != nil {
		t.Fatal(err)
	}

	if nRemoved != 1 {
		t.Fatal("expected file to be removed")
	}

	nRemoved, err = fs.RemoveExpiredUnlinked(time.Duration(0))
	if err != nil {
		t.Fatal(err)
	}

	if nRemoved != 0 {
		t.Fatal("nothing to be removed")
	}
}

func TestRenameSameDir(t *testing.T) {
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
	fs := tmpFs(t)

	foo1Stat, err := fs.Mknod(ROOT_INO, "foo1", MknodOpts{
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

	if bar1Stat.Ino != foo1Stat.Ino {
		t.Fatalf("bar1 stat is bad: %#v", bar1Stat)
	}

	nRemoved, err := fs.RemoveExpiredUnlinked(time.Duration(0))
	if err != nil {
		t.Fatal(err)
	}

	if nRemoved != 1 {
		t.Fatal("expected file to be removed")
	}

}

func TestRenameDifferentDir(t *testing.T) {
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

	nRemoved, err := fs.RemoveExpiredUnlinked(time.Duration(0))
	if err != nil {
		t.Fatal(err)
	}

	if nRemoved != 1 {
		t.Fatal("expected file to be removed")
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

func TestWriteDataOneChunk(t *testing.T) {
	fs := tmpFs(t)

	testSizes := []uint64{0, 1, 3, CHUNK_SIZE - 1, CHUNK_SIZE}

	for i, n := range testSizes {
		name := fmt.Sprintf("d%d", i)
		stat, err := fs.Mknod(ROOT_INO, name, MknodOpts{
			Mode: S_IFREG | 0o777,
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

		nWritten, err := fs.WriteData(stat.Ino, data, 0)
		if err != nil {
			t.Fatal(err)
		}
		if nWritten != uint32(len(data)) {
			t.Fatalf("unexpected write amount %d != %d", nWritten, len(data))
		}

		fetchedData, err := fs.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
			data := tx.Get(tuple.Tuple{"fs", "ino", stat.Ino, "data", 0}).MustGet()
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
	fs := tmpFs(t)

	testSizes := []uint64{CHUNK_SIZE + 1, CHUNK_SIZE + 3, CHUNK_SIZE * 2}

	for i, n := range testSizes {
		name := fmt.Sprintf("d%d", i)
		stat, err := fs.Mknod(ROOT_INO, name, MknodOpts{
			Mode: S_IFREG | 0o777,
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

		data1 := data[:CHUNK_SIZE]
		data2 := data[CHUNK_SIZE:]

		nWritten := 0
		for nWritten != len(data) {
			n, err := fs.WriteData(stat.Ino, data[nWritten:], uint64(nWritten))
			if err != nil {
				t.Fatal(err)
			}
			nWritten += int(n)
		}

		fetchedData1, err := fs.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
			data := tx.Get(tuple.Tuple{"fs", "ino", stat.Ino, "data", 0}).MustGet()
			zeroExpandChunk(&data)
			return data, nil
		})
		if err != nil {
			t.Fatal(err)
		}

		fetchedData2, err := fs.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
			data := tx.Get(tuple.Tuple{"fs", "ino", stat.Ino, "data", 1}).MustGet()
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
		stat, err := fs.Mknod(ROOT_INO, name, MknodOpts{
			Mode: S_IFREG | 0o777,
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
			n, err := fs.WriteData(stat.Ino, data[nWritten:], uint64(nWritten))
			if err != nil {
				t.Fatal(err)
			}
			nWritten += int(n)
		}

		_, err = fs.ModStat(stat.Ino, ModStatOpts{
			Valid: SETSTAT_SIZE,
			Size:  5,
		})
		if err != nil {
			t.Fatal(err)
		}

		_, err = fs.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
			kvs := tx.GetRange(tuple.Tuple{"fs", "ino", stat.Ino, "data"}, fdb.RangeOptions{}).GetSliceOrPanic()
			if len(kvs) != 1 {
				t.Fatalf("bad number of data chunks: %d", len(kvs))
			}
			data := tx.Get(tuple.Tuple{"fs", "ino", stat.Ino, "data", 0}).MustGet()
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
	fs := tmpFs(t)

	// Random writes at different offsets to exercise the sparse code paths.
	for i := 0; i < 100; i++ {
		stat, err := fs.Mknod(ROOT_INO, "f", MknodOpts{
			Mode: S_IFREG | 0o777,
			Uid:  0,
			Gid:  0,
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
				n, err := fs.WriteData(stat.Ino, writeData[nWritten:], uint64(writeOffset)+uint64(nWritten))
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
			n, err := fs.ReadData(stat.Ino, readBuf, nRead)
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
