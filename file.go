package hafs

import (
	"bytes"
	"io"
	"os"
	"sync"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

type HafsFile interface {
	WriteData([]byte, uint64) (uint32, error)
	ReadData([]byte, uint64) (uint32, error)
	Fsync() error
	Close() error
}

type invalidFile struct{}

func (f *invalidFile) WriteData(buf []byte, offset uint64) (uint32, error) { return 0, ErrInvalid }
func (f *invalidFile) ReadData(buf []byte, offset uint64) (uint32, error)  { return 0, ErrInvalid }
func (f *invalidFile) Fsync() error                                        { return ErrInvalid }
func (f *invalidFile) Close() error                                        { return nil }

type fdbBackedFile struct {
	fs  *Fs
	ino uint64
}

func zeroTrimPage(page []byte) []byte {
	i := len(page) - 1
	for ; i >= 0; i-- {
		if page[i] != 0 {
			break
		}
	}
	return page[:i+1]
}

var _zeroPage [PAGE_SIZE]byte

func zeroExpandPage(page *[]byte) {
	*page = append(*page, _zeroPage[len(*page):PAGE_SIZE]...)
}

func (f *fdbBackedFile) WriteData(buf []byte, offset uint64) (uint32, error) {
	const MAX_WRITE = 32 * PAGE_SIZE

	// FoundationDB has a transaction time limit and a transaction size limit,
	// limit the write to something that can fit.
	if len(buf) > MAX_WRITE {
		buf = buf[:MAX_WRITE]
	}

	nWritten, err := f.fs.Transact(func(tx fdb.Transaction) (interface{}, error) {

		futureStat := f.fs.txGetStat(tx, f.ino)
		currentOffset := offset
		remainingBuf := buf

		// Deal with the first unaligned and undersized pages.
		if currentOffset%PAGE_SIZE != 0 || len(remainingBuf) < PAGE_SIZE {
			firstPageNo := currentOffset / PAGE_SIZE
			firstPageOffset := currentOffset % PAGE_SIZE
			firstWriteCount := PAGE_SIZE - firstPageOffset
			if firstWriteCount > uint64(len(buf)) {
				firstWriteCount = uint64(len(buf))
			}
			firstPageKey := tuple.Tuple{"hafs", f.fs.fsName, "ino", f.ino, "data", firstPageNo}
			page := tx.Get(firstPageKey).MustGet()
			zeroExpandPage(&page)
			copy(page[firstPageOffset:firstPageOffset+firstWriteCount], remainingBuf)
			currentOffset += firstWriteCount
			remainingBuf = remainingBuf[firstWriteCount:]
			tx.Set(firstPageKey, zeroTrimPage(page))
		}

		for {
			key := tuple.Tuple{"hafs", f.fs.fsName, "ino", f.ino, "data", currentOffset / PAGE_SIZE}
			if len(remainingBuf) >= PAGE_SIZE {
				tx.Set(key, zeroTrimPage(remainingBuf[:PAGE_SIZE]))
				currentOffset += PAGE_SIZE
				remainingBuf = remainingBuf[PAGE_SIZE:]
			} else {
				page := tx.Get(key).MustGet()
				zeroExpandPage(&page)
				copy(page, remainingBuf)
				tx.Set(key, zeroTrimPage(page))
				currentOffset += uint64(len(remainingBuf))
				break
			}
		}

		stat, err := futureStat.Get()
		if err != nil {
			return nil, err
		}

		if stat.Mode&S_IFMT != S_IFREG {
			return nil, ErrInvalid
		}

		nWritten := currentOffset - offset

		if stat.Size < offset+nWritten {
			newSize := offset + nWritten
			if stat.Nlink != 0 {
				f.fs.txSubvolumeByteDelta(tx, stat.Subvolume, int64(newSize)-int64(stat.Size))
			}
			stat.Size = newSize
		}
		stat.SetMtime(time.Now())
		f.fs.txSetStat(tx, stat)
		return uint32(nWritten), nil
	})
	if err != nil {
		return 0, err
	}
	return nWritten.(uint32), nil
}
func (f *fdbBackedFile) ReadData(buf []byte, offset uint64) (uint32, error) {

	const MAX_READ = 32 * PAGE_SIZE

	if len(buf) > MAX_READ {
		buf = buf[:MAX_READ]
	}

	nRead, err := f.fs.Transact(func(tx fdb.Transaction) (interface{}, error) {
		currentOffset := offset
		remainingBuf := buf

		stat, err := f.fs.txGetStat(tx, f.ino).Get()
		if err != nil {
			return nil, err
		}

		if stat.Mode&S_IFMT != S_IFREG {
			return nil, ErrInvalid
		}

		if offset >= stat.Size {
			return 0, io.EOF
		}

		// Don't read past the end of the file.
		if stat.Size < currentOffset+uint64(len(remainingBuf)) {
			overshoot := (currentOffset + uint64(len(remainingBuf))) - stat.Size
			remainingBuf = remainingBuf[:uint64(len(remainingBuf))-overshoot]
		}

		// Deal with the first unaligned and undersized page.
		if currentOffset%PAGE_SIZE != 0 || len(remainingBuf) < PAGE_SIZE {

			firstPageNo := currentOffset / PAGE_SIZE
			firstPageOffset := currentOffset % PAGE_SIZE
			firstReadCount := PAGE_SIZE - firstPageOffset
			if firstReadCount > uint64(len(remainingBuf)) {
				firstReadCount = uint64(len(remainingBuf))
			}

			firstPageKey := tuple.Tuple{"hafs", f.fs.fsName, "ino", f.ino, "data", firstPageNo}
			page := tx.Get(firstPageKey).MustGet()
			if page != nil {
				zeroExpandPage(&page)
				copy(remainingBuf[:firstReadCount], page[firstPageOffset:firstPageOffset+firstReadCount])
			} else {
				// Sparse read.
				for i := uint64(0); i < firstReadCount; i += 1 {
					remainingBuf[i] = 0
				}
			}
			remainingBuf = remainingBuf[firstReadCount:]
			currentOffset += firstReadCount
		}

		nPages := uint64(len(remainingBuf)) / PAGE_SIZE
		if (len(remainingBuf) % PAGE_SIZE) != 0 {
			nPages += 1
		}
		pageFutures := make([]fdb.FutureByteSlice, 0, nPages)

		// Read all pages in parallel using futures.
		for i := uint64(0); i < nPages; i++ {
			key := tuple.Tuple{"hafs", f.fs.fsName, "ino", f.ino, "data", (currentOffset / PAGE_SIZE) + i}
			pageFutures = append(pageFutures, tx.Get(key))
		}

		for i := uint64(0); i < nPages; i++ {
			page := pageFutures[i].MustGet()
			zeroExpandPage(&page)
			n := copy(remainingBuf, page)
			currentOffset += uint64(n)
			remainingBuf = remainingBuf[n:]
		}

		nRead := currentOffset - offset

		return uint32(nRead), nil
	})
	nReadInt, ok := nRead.(uint32)
	if ok {
		return nReadInt, err
	} else {
		return 0, err
	}
}
func (f *fdbBackedFile) Fsync() error { return nil }
func (f *fdbBackedFile) Close() error { return nil }

type objectStoreReadOnlyFile struct {
	fs         *Fs
	ino        uint64
	size       uint64
	parts      uint64
	fetched    AtomicBitset
	fetchMutex sync.Mutex
	rdata      io.ReaderAt
	wdata      *os.File
}

func newObjectStoreReadOnlyFile(fs *Fs, ino, size uint64) (*objectStoreReadOnlyFile, error) {

	parts := size / fs.objectPartSize
	if size%fs.objectPartSize != 0 {
		parts += 1
	}

	if parts == 1 {
		return &objectStoreReadOnlyFile{
			fs:      fs,
			ino:     ino,
			size:    size,
			parts:   parts,
			fetched: NewAtomicBitset(uint(parts)),
		}, nil
	}

	tmpFile, err := os.CreateTemp("", "")
	if err != nil {
		return nil, err
	}

	// XXX Make file anonymous, it would be nice to create it like this.
	err = os.Remove(tmpFile.Name())
	if err != nil {
		return nil, err
	}

	return &objectStoreReadOnlyFile{
		fs:      fs,
		ino:     ino,
		size:    size,
		parts:   parts,
		fetched: NewAtomicBitset(uint(parts)),
		rdata:   tmpFile,
		wdata:   tmpFile,
	}, nil

}

func (f *objectStoreReadOnlyFile) WriteData(buf []byte, offset uint64) (uint32, error) {
	return 0, ErrNotSupported
}

func (f *objectStoreReadOnlyFile) ReadData(buf []byte, offset uint64) (uint32, error) {

	if offset >= f.size {
		return 0, io.EOF
	}

	partIndex := offset / f.fs.objectPartSize
	partStartOffset := partIndex * f.fs.objectPartSize
	partOffset := offset - partStartOffset
	partSize := uint64(f.fs.objectPartSize)

	if partIndex+1 == f.parts {
		// This is the last part, it is truncated.
		partSize = f.size - partStartOffset
	}

	// Keep the read within the bounds of this part.
	if partOffset+uint64(len(buf)) > partSize {
		overshoot := partOffset + uint64(len(buf)) - partSize
		buf = buf[:uint64(len(buf))-overshoot]
	}

	if f.fetched.IsSet(uint(partIndex)) {
		n, err := f.rdata.ReadAt(buf, int64(offset))
		return uint32(n), err
	}

	f.fetchMutex.Lock()

	if f.fetched.IsSet(uint(partIndex)) {
		// Someone else fetched this chunk while we were waiting.
		f.fetchMutex.Unlock()
		n, err := f.rdata.ReadAt(buf, int64(offset))
		return uint32(n), err
	}

	part := make([]byte, partSize)

	_, err := f.fs.objectStorage.Read(f.fs.fsName, f.ino, partStartOffset, part)
	if err != nil {
		f.fetchMutex.Unlock()
		return 0, err
	}

	if f.parts > 1 {
		_, err = f.wdata.WriteAt(part, int64(partStartOffset))
		if err != nil {
			f.fetchMutex.Unlock()
			return 0, err
		}
	} else {
		// As an optimisation, when the file has a single part, just read from memory.
		f.rdata = bytes.NewReader(part)
	}

	f.fetched.Set(uint(partIndex))
	f.fetchMutex.Unlock()

	n := copy(buf, part[partOffset:])
	return uint32(n), nil

}

func (f *objectStoreReadOnlyFile) Fsync() error {
	return nil
}

func (f *objectStoreReadOnlyFile) Close() error {
	if f.wdata != nil {
		_ = f.wdata.Close()
	}
	return nil
}

type objectStoreReadWriteFile struct {
	fs    *Fs
	dirty atomicBool
	ino   uint64
	data  *os.File
}

func newObjectStoreReadWriteFile(fs *Fs, ino uint64) (*objectStoreReadWriteFile, error) {
	tmpFile, err := os.CreateTemp("", "")
	if err != nil {
		return nil, err
	}

	// XXX Make file anonymous, it would be nice to create it like this.
	err = os.Remove(tmpFile.Name())
	if err != nil {
		return nil, err
	}

	return &objectStoreReadWriteFile{
		fs:   fs,
		ino:  ino,
		data: tmpFile,
	}, nil
}

func (f *objectStoreReadWriteFile) WriteData(buf []byte, offset uint64) (uint32, error) {
	f.dirty.Store(true)
	n, err := f.data.WriteAt(buf, int64(offset))
	return uint32(n), err
}

func (f *objectStoreReadWriteFile) ReadData(buf []byte, offset uint64) (uint32, error) {
	n, err := f.data.ReadAt(buf, int64(offset))
	return uint32(n), err
}

func (f *objectStoreReadWriteFile) Fsync() error {
	if !f.dirty.Load() {
		return nil
	}

	// FIXME, XXX - the file size could change if written in parallel with fsync.
	dataStat, err := f.data.Stat()
	if err != nil {
		return err
	}

	stat := Stat{}

	_, err = f.fs.Transact(func(tx fdb.Transaction) (interface{}, error) {
		stat, err = f.fs.txGetStat(tx, f.ino).Get()
		if err != nil {
			return nil, err
		}
		if stat.Size != 0 {
			// Object storage files can only be written once.
			return nil, ErrNotSupported
		}

		stat.Size = uint64(dataStat.Size())
		if stat.Nlink != 0 {
			f.fs.txSubvolumeByteDelta(tx, stat.Subvolume, int64(stat.Size))
		}
		f.fs.txSetStat(tx, stat)
		return nil, nil
	})
	if err != nil {
		return err
	}

	if stat.Size == 0 || stat.Nlink == 0 {
		// No point in uploading an empty object and
		// uploading an unlinked object creates orphan objects.
		f.dirty.Store(false)
		return nil
	}

	_, err = f.fs.objectStorage.Write(f.fs.fsName, f.ino, f.data)
	if err != nil {
		return err
	}

	f.dirty.Store(false)
	return nil
}

func (f *objectStoreReadWriteFile) Close() error {
	_ = f.data.Close()
	return nil
}
