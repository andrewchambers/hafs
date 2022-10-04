package hafs

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
)

var (
	ErrNotExist     = unix.ENOENT
	ErrExist        = unix.EEXIST
	ErrNotEmpty     = unix.ENOTEMPTY
	ErrNotDir       = unix.ENOTDIR
	ErrInvalid      = unix.EINVAL
	ErrNotSupported = unix.ENOTSUP
	ErrPermission   = unix.EPERM
	ErrIntr         = unix.EINTR
	ErrUnmounted    = errors.New("filesystem unmounted")
)

const (
	CURRENT_FDB_API_VERSION = 600
	CURRENT_SCHEMA_VERSION  = 1
	ROOT_INO                = 1
	CHUNK_SIZE              = 4096
	INO_BATCH_SIZE          = 65536
	INO_CHAN_SIZE           = 16384
)

const (
	S_IFIFO  uint32 = unix.S_IFIFO
	S_IFCHR  uint32 = unix.S_IFCHR
	S_IFBLK  uint32 = unix.S_IFBLK
	S_IFDIR  uint32 = unix.S_IFDIR
	S_IFREG  uint32 = unix.S_IFREG
	S_IFLNK  uint32 = unix.S_IFLNK
	S_IFSOCK uint32 = unix.S_IFSOCK
	S_IFMT   uint32 = unix.S_IFMT
)

type DirEnt struct {
	Name string `json:"-"`
	Mode uint32 // Mode & S_IFMT
	Ino  uint64
}

type Stat struct {
	Ino       uint64 `json:"-"`
	Size      uint64
	Atimesec  uint64
	Mtimesec  uint64
	Ctimesec  uint64
	Atimensec uint32
	Mtimensec uint32
	Ctimensec uint32
	Mode      uint32
	Nlink     uint32
	Uid       uint32
	Gid       uint32
	Rdev      uint32
	Storage   string `json:",omitempty"`
}

func (stat *Stat) setTime(t time.Time, secs *uint64, nsecs *uint32) {
	*secs = uint64(t.UnixNano() / 1_000_000_000)
	*nsecs = uint32(t.UnixNano() % 1_000_000_000)
}

func (stat *Stat) SetMtime(t time.Time) {
	stat.setTime(t, &stat.Mtimesec, &stat.Mtimensec)
}

func (stat *Stat) SetAtime(t time.Time) {
	stat.setTime(t, &stat.Atimesec, &stat.Atimensec)
}

func (stat *Stat) SetCtime(t time.Time) {
	stat.setTime(t, &stat.Ctimesec, &stat.Ctimensec)
}

func (stat *Stat) Mtime() time.Time {
	return time.Unix(int64(stat.Mtimesec), int64(stat.Mtimensec))
}

func (stat *Stat) Atime() time.Time {
	return time.Unix(int64(stat.Atimesec), int64(stat.Atimensec))
}

func (stat *Stat) Ctime() time.Time {
	return time.Unix(int64(stat.Ctimesec), int64(stat.Ctimensec))
}

type Fs struct {
	db                  fdb.Database
	mountId             string
	onEviction          func(fs *Fs)
	clientDetached      atomicBool
	inoChan             chan uint64
	dirRelMtimeDuration time.Duration // TODO XXX make an option.
	workerWg            *sync.WaitGroup
	cancelWorkers       func()
}

func init() {
	fdb.MustAPIVersion(CURRENT_FDB_API_VERSION)
}

type MkfsOpts struct {
	Overwrite bool
}

func Mkfs(db fdb.Database, opts MkfsOpts) error {
	_, err := db.Transact(func(tx fdb.Transaction) (interface{}, error) {

		if tx.Get(tuple.Tuple{"fs", "version"}).MustGet() != nil {
			if !opts.Overwrite {
				return nil, errors.New("filesystem already present")
			}
		}

		now := time.Now()

		rootStat := Stat{
			Ino:       ROOT_INO,
			Size:      0,
			Atimesec:  0,
			Mtimesec:  0,
			Ctimesec:  0,
			Atimensec: 0,
			Mtimensec: 0,
			Ctimensec: 0,
			Mode:      S_IFDIR | 0o755,
			Nlink:     1,
			Uid:       0,
			Gid:       0,
			Rdev:      0,
		}

		rootStat.SetMtime(now)
		rootStat.SetCtime(now)
		rootStat.SetAtime(now)

		rootStatBytes, err := json.Marshal(rootStat)
		if err != nil {
			return nil, err
		}

		tx.ClearRange(tuple.Tuple{"fs"})
		tx.Set(tuple.Tuple{"fs", "version"}, []byte{CURRENT_SCHEMA_VERSION})
		tx.Set(tuple.Tuple{"fs", "ino", ROOT_INO, "stat"}, rootStatBytes)
		tx.Set(tuple.Tuple{"fs", "nextino"}, []byte("2"))
		return nil, nil
	})
	return err
}

type AttachOpts struct {
	ClientDescription string
	OnEviction        func(fs *Fs)
}

func Attach(db fdb.Database, opts AttachOpts) (*Fs, error) {
	hostname, _ := os.Hostname()

	exe, _ := os.Executable()

	if opts.ClientDescription == "" {
		if idx := strings.LastIndex(exe, "/"); idx != -1 {
			opts.ClientDescription = exe[idx+1:]
		} else {
			opts.ClientDescription = exe
		}
	}

	if opts.OnEviction == nil {
		opts.OnEviction = func(fs *Fs) {}
	}

	idBytes := [16]byte{}
	_, err := rand.Read(idBytes[:])
	if err != nil {
		return nil, err
	}
	mountId := hex.EncodeToString(idBytes[:])

	now := time.Now()

	clientInfo := ClientInfo{
		Pid:            int64(os.Getpid()),
		Exe:            exe,
		Description:    opts.ClientDescription,
		Hostname:       hostname,
		AttachTimeUnix: uint64(now.Unix()),
	}

	clientInfoBytes, err := json.Marshal(&clientInfo)
	if err != nil {
		return nil, err
	}

	initialHeartBeatBytes, err := json.Marshal(now.Unix())
	if err != nil {
		return nil, err
	}

	_, err = db.Transact(func(tx fdb.Transaction) (interface{}, error) {
		version := tx.Get(tuple.Tuple{"fs", "version"}).MustGet()
		if version == nil {
			return nil, errors.New("filesystem is not formatted")
		}
		if !bytes.Equal(version, []byte{CURRENT_SCHEMA_VERSION}) {
			return nil, fmt.Errorf("filesystem has different version - expected %d but got %d", CURRENT_SCHEMA_VERSION, version[0])
		}

		tx.Set(tuple.Tuple{"fs", "mount", mountId, "info"}, clientInfoBytes)
		tx.Set(tuple.Tuple{"fs", "mount", mountId, "heartbeat"}, initialHeartBeatBytes)
		tx.Set(tuple.Tuple{"fs", "mount", mountId, "attached"}, []byte{1})
		tx.Set(tuple.Tuple{"fs", "mounts", mountId}, []byte{})
		return nil, nil
	})
	if err != nil {
		return nil, fmt.Errorf("unable to add mount: %w", err)
	}

	workerCtx, cancelWorkers := context.WithCancel(context.Background())

	fs := &Fs{
		db:                  db,
		onEviction:          opts.OnEviction,
		dirRelMtimeDuration: 24 * time.Hour,
		mountId:             mountId,
		cancelWorkers:       cancelWorkers,
		workerWg:            &sync.WaitGroup{},
		inoChan:             make(chan uint64, INO_CHAN_SIZE),
	}

	fs.workerWg.Add(1)
	go func() {
		defer fs.workerWg.Done()
		fs.requestInosForever(workerCtx)
	}()

	fs.workerWg.Add(1)
	go func() {
		defer fs.workerWg.Done()
		fs.mountHeartBeatForever(workerCtx)
	}()

	return fs, nil
}

func reservedIno(ino uint64) bool {
	// XXX Why is this 4 bytes?
	const FUSE_UNKNOWN_INO = 0xFFFFFFFF
	// XXX We currently reserve this inode too pending an answer to https://github.com/hanwen/go-fuse/issues/439.
	const RESERVED_INO_1 = 0xFFFFFFFFFFFFFFFF
	return ino == FUSE_UNKNOWN_INO || ino == RESERVED_INO_1
}

func (fs *Fs) nextIno() (uint64, error) {
	// XXX This is a single bottleneck, could we shard or use atomics?
	for {
		ino, ok := <-fs.inoChan
		if !ok {
			return 0, ErrUnmounted
		}
		if !reservedIno(ino) {
			return ino, nil
		}
	}
}

func (fs *Fs) requestInosForever(ctx context.Context) {
	nextInoKey := tuple.Tuple{"fs", "nextino"}
	for {
		v, err := fs.Transact(func(tx fdb.Transaction) (interface{}, error) {
			nextInoBytes := tx.Get(nextInoKey).MustGet()
			var currentIno uint64
			err := json.Unmarshal(nextInoBytes, &currentIno)
			if err != nil {
				return nil, err
			}
			nextIno := currentIno + INO_BATCH_SIZE
			if nextIno >= 0x7FFFFFFFFFFFFFFF {
				// Don't allow inodes to get into the range where
				// they can't be represented by int64 or uint64 -
				// We do this because FoundationDB encodes inodes
				// in key tuples as int64.
				panic("inodes exhausted")
			}
			nextInoBytes, err = json.Marshal(nextIno)
			if err != nil {
				return nil, err
			}
			tx.Set(nextInoKey, nextInoBytes)
			return currentIno, nil
		})
		if err != nil {
			log.Printf("unable to allocate inode batch: %s", err)
			time.Sleep(1 * time.Second)
			continue
		}

		currentIno := v.(uint64)
		for currentIno < currentIno+INO_BATCH_SIZE {
			select {
			case fs.inoChan <- currentIno:
			default:
				select {
				case fs.inoChan <- currentIno:
				case <-ctx.Done():
					close(fs.inoChan)
					return
				}
			}
			currentIno += 1
		}
	}
}

func (fs *Fs) mountHeartBeat() error {
	_, err := fs.Transact(func(tx fdb.Transaction) (interface{}, error) {
		heartBeatKey := tuple.Tuple{"fs", "mount", fs.mountId, "heartbeat"}
		lastSeen, err := json.Marshal(time.Now().Unix())
		if err != nil {
			return nil, err
		}
		tx.Set(heartBeatKey, lastSeen)
		return nil, nil
	})
	return err
}

func (fs *Fs) mountHeartBeatForever(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			err := fs.mountHeartBeat()
			if errors.Is(err, ErrUnmounted) {
				// Must be done in new goroutine to prevent deadlock.
				go fs.onEviction(fs)
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (fs *Fs) IsDetatched() bool {
	return fs.clientDetached.Load()
}

func (fs *Fs) Close() error {
	fs.cancelWorkers()
	fs.workerWg.Wait()

	err := fs.EvictClient(fs.mountId)
	return err
}

func (fs *Fs) ReadTransact(f func(tx fdb.ReadTransaction) (interface{}, error)) (interface{}, error) {
	return fs.db.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
		mountCheck := tx.Get(tuple.Tuple{"fs", "mount", fs.mountId, "attached"})
		v, err := f(tx)
		if mountCheck.MustGet() == nil {
			return v, ErrUnmounted
		}
		return v, err
	})
}

func (fs *Fs) Transact(f func(tx fdb.Transaction) (interface{}, error)) (interface{}, error) {
	return fs.db.Transact(func(tx fdb.Transaction) (interface{}, error) {
		mountCheck := tx.Get(tuple.Tuple{"fs", "mount", fs.mountId, "attached"})
		v, err := f(tx)
		if mountCheck.MustGet() == nil {
			return v, ErrUnmounted
		}
		return v, err
	})
}

type futureStat struct {
	ino   uint64
	bytes fdb.FutureByteSlice
}

func (fut futureStat) Get() (Stat, error) {
	stat := Stat{}
	statBytes := fut.bytes.MustGet()
	if statBytes == nil {
		return stat, ErrNotExist
	}

	err := json.Unmarshal(statBytes, &stat)
	if err != nil {
		return stat, err
	}
	stat.Ino = fut.ino
	return stat, nil
}

func (fs *Fs) txGetStat(tx fdb.ReadTransaction, ino uint64) futureStat {
	return futureStat{
		ino:   ino,
		bytes: tx.Get(tuple.Tuple{"fs", "ino", ino, "stat"}),
	}
}

func (fs *Fs) txSetStat(tx fdb.Transaction, stat Stat) {
	statBytes, err := json.Marshal(stat)
	if err != nil {
		panic(err)
	}
	tx.Set(tuple.Tuple{"fs", "ino", stat.Ino, "stat"}, statBytes)
}

type futureGetDirEnt struct {
	name  string
	bytes fdb.FutureByteSlice
}

func (fut futureGetDirEnt) Get() (DirEnt, error) {
	dirEntBytes := fut.bytes.MustGet()
	if dirEntBytes == nil {
		return DirEnt{}, ErrNotExist
	}
	dirEnt := DirEnt{}
	err := json.Unmarshal(dirEntBytes, &dirEnt)
	dirEnt.Name = fut.name
	return dirEnt, err
}

func (fs *Fs) txGetDirEnt(tx fdb.ReadTransaction, dirIno uint64, name string) futureGetDirEnt {
	return futureGetDirEnt{
		name:  name,
		bytes: tx.Get(tuple.Tuple{"fs", "ino", dirIno, "child", name}),
	}
}

func (fs *Fs) txSetDirEnt(tx fdb.Transaction, dirIno uint64, ent DirEnt) {
	dirEntBytes, err := json.Marshal(ent)
	if err != nil {
		panic(err)
	}
	tx.Set(tuple.Tuple{"fs", "ino", dirIno, "child", ent.Name}, dirEntBytes)
}

func (fs *Fs) GetDirEnt(dirIno uint64, name string) (DirEnt, error) {
	dirEnt, err := fs.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
		dirEnt, err := fs.txGetDirEnt(tx, dirIno, name).Get()
		return dirEnt, err
	})
	if err != nil {
		return DirEnt{}, err
	}
	return dirEnt.(DirEnt), nil
}

func (fs *Fs) txDirHasChildren(tx fdb.ReadTransaction, dirIno uint64) bool {
	kvs := tx.GetRange(tuple.Tuple{"fs", "ino", dirIno, "child"}, fdb.RangeOptions{
		Limit: 1,
	}).GetSliceOrPanic()
	return len(kvs) != 0
}

type MknodOpts struct {
	Truncate   bool
	Mode       uint32
	Uid        uint32
	Gid        uint32
	Rdev       uint32
	LinkTarget []byte
}

func (fs *Fs) txMknod(tx fdb.Transaction, dirIno uint64, name string, opts MknodOpts) (Stat, error) {
	dirStatFut := fs.txGetStat(tx, dirIno)
	getDirEntFut := fs.txGetDirEnt(tx, dirIno, name)

	dirStat, err := dirStatFut.Get()
	if err != nil {
		return Stat{}, err
	}

	if dirStat.Mode&S_IFMT != S_IFDIR {
		return Stat{}, ErrNotDir
	}

	var stat Stat

	existingDirEnt, err := getDirEntFut.Get()
	if err == nil {
		if !opts.Truncate {
			return Stat{}, ErrExist
		}
		if existingDirEnt.Mode&S_IFMT != S_IFREG {
			return Stat{}, ErrInvalid
		}
		stat, err = fs.txModStat(tx, existingDirEnt.Ino, ModStatOpts{Valid: MODSTAT_SIZE, Size: 0})
		if err != nil {
			return Stat{}, err
		}
	} else if err != ErrNotExist {
		return Stat{}, err
	} else {
		newIno, err := fs.nextIno()
		if err != nil {
			return Stat{}, err
		}
		stat = Stat{
			Ino:       newIno,
			Size:      0,
			Atimesec:  0,
			Mtimesec:  0,
			Ctimesec:  0,
			Atimensec: 0,
			Mtimensec: 0,
			Ctimensec: 0,
			Mode:      opts.Mode,
			Nlink:     1,
			Uid:       opts.Uid,
			Gid:       opts.Gid,
			Rdev:      opts.Rdev,
			Storage:   "",
		}
		if opts.Mode&S_IFMT == S_IFREG {
			// Only files inherit storage from the parent directory.
			stat.Storage = dirStat.Storage
		}
	}

	now := time.Now()
	stat.SetMtime(now)
	stat.SetCtime(now)
	stat.SetAtime(now)
	fs.txSetStat(tx, stat)
	fs.txSetDirEnt(tx, dirIno, DirEnt{
		Name: name,
		Mode: stat.Mode & S_IFMT,
		Ino:  stat.Ino,
	})

	if dirStat.Mtime().Before(now.Add(-fs.dirRelMtimeDuration)) {
		dirStat.SetMtime(now)
		dirStat.SetAtime(now)
		fs.txSetStat(tx, dirStat)
	}

	if stat.Mode&S_IFMT == S_IFLNK {
		tx.Set(tuple.Tuple{"fs", "ino", stat.Ino, "target"}, opts.LinkTarget)
	}

	return stat, nil
}

func (fs *Fs) Mknod(dirIno uint64, name string, opts MknodOpts) (Stat, error) {
	stat, err := fs.Transact(func(tx fdb.Transaction) (interface{}, error) {
		stat, err := fs.txMknod(tx, dirIno, name, opts)
		return stat, err
	})
	if err != nil {
		return Stat{}, err
	}
	return stat.(Stat), nil
}

func (fs *Fs) HardLink(dirIno, ino uint64, name string) (Stat, error) {
	stat, err := fs.Transact(func(tx fdb.Transaction) (interface{}, error) {

		dirStatFut := fs.txGetStat(tx, dirIno)
		dirEntFut := fs.txGetDirEnt(tx, dirIno, name)
		statFut := fs.txGetStat(tx, ino)

		dirStat, err := dirStatFut.Get()
		if err != nil {
			return Stat{}, err
		}
		if dirStat.Mode&S_IFMT != S_IFDIR {
			return Stat{}, ErrNotDir
		}

		stat, err := statFut.Get()
		if err != nil {
			return Stat{}, err
		}
		// Can't hardlink directories.
		if stat.Mode&S_IFMT == S_IFDIR {
			return Stat{}, ErrPermission
		}
		if stat.Nlink == 0 {
			// Don't resurrect inodes.
			return Stat{}, ErrInvalid
		}

		_, err = dirEntFut.Get()
		if err == nil {
			return Stat{}, ErrExist
		}
		if err != ErrNotExist {
			return Stat{}, err
		}

		now := time.Now()

		stat.SetAtime(now)
		stat.SetCtime(now)
		stat.Nlink += 1
		fs.txSetStat(tx, stat)

		fs.txSetDirEnt(tx, dirIno, DirEnt{
			Name: name,
			Mode: stat.Mode & S_IFMT,
			Ino:  stat.Ino,
		})

		if dirStat.Mtime().Before(now.Add(-fs.dirRelMtimeDuration)) {
			dirStat.SetMtime(now)
			dirStat.SetAtime(now)
			fs.txSetStat(tx, dirStat)
		}

		return stat, nil
	})
	if err != nil {
		return Stat{}, err
	}
	return stat.(Stat), nil
}

func (fs *Fs) Unlink(dirIno uint64, name string) error {
	_, err := fs.Transact(func(tx fdb.Transaction) (interface{}, error) {
		dirStatFut := fs.txGetStat(tx, dirIno)

		dirEnt, err := fs.txGetDirEnt(tx, dirIno, name).Get()
		if err != nil {
			return nil, err
		}
		stat, err := fs.txGetStat(tx, dirEnt.Ino).Get()
		if err != nil {
			return nil, err
		}

		dirStat, err := dirStatFut.Get()
		if err != nil {
			return nil, err
		}

		if dirEnt.Mode&S_IFMT == S_IFDIR {
			if fs.txDirHasChildren(tx, stat.Ino) {
				return nil, ErrNotEmpty
			}
		}

		now := time.Now()
		dirStat.SetMtime(now)
		dirStat.SetCtime(now)
		fs.txSetStat(tx, dirStat)
		stat.Nlink -= 1
		stat.SetMtime(now)
		stat.SetCtime(now)
		fs.txSetStat(tx, stat)
		if stat.Nlink == 0 {
			tx.Set(tuple.Tuple{"fs", "unlinked", dirEnt.Ino}, []byte{})
		}
		tx.Clear(tuple.Tuple{"fs", "ino", dirIno, "child", name})
		return nil, nil
	})
	return err
}

type HafsFile interface {
	WriteData([]byte, uint64) (uint32, error)
	ReadData([]byte, uint64) (uint32, error)
	Fsync() error
	Flush() error
	Close() error
}

type foundationDBFile struct {
	fs  *Fs
	ino uint64
}

func zeroTrimChunk(chunk []byte) []byte {
	i := len(chunk) - 1
	for ; i >= 0; i-- {
		if chunk[i] != 0 {
			break
		}
	}
	return chunk[:i+1]
}

var _zeroChunk [CHUNK_SIZE]byte

func zeroExpandChunk(chunk *[]byte) {
	*chunk = append(*chunk, _zeroChunk[len(*chunk):CHUNK_SIZE]...)
}

func (f *foundationDBFile) WriteData(buf []byte, offset uint64) (uint32, error) {
	const MAX_WRITE = 128 * CHUNK_SIZE

	// FoundationDB has a transaction time limit and a transaction size limit,
	// limit the write to something that can fit.
	if len(buf) > MAX_WRITE {
		buf = buf[:MAX_WRITE]
	}

	nWritten, err := f.fs.Transact(func(tx fdb.Transaction) (interface{}, error) {

		futureStat := f.fs.txGetStat(tx, f.ino)
		currentOffset := offset
		remainingBuf := buf

		// Deal with the first unaligned and undersized chunks.
		if currentOffset%CHUNK_SIZE != 0 || len(remainingBuf) < CHUNK_SIZE {
			firstChunkNo := currentOffset / CHUNK_SIZE
			firstChunkOffset := currentOffset % CHUNK_SIZE
			firstWriteCount := CHUNK_SIZE - firstChunkOffset
			if firstWriteCount > uint64(len(buf)) {
				firstWriteCount = uint64(len(buf))
			}
			firstChunkKey := tuple.Tuple{"fs", "ino", f.ino, "data", firstChunkNo}
			chunk := tx.Get(firstChunkKey).MustGet()
			if chunk == nil {
				chunk = make([]byte, CHUNK_SIZE, CHUNK_SIZE)
			} else {
				zeroExpandChunk(&chunk)
			}
			copy(chunk[firstChunkOffset:firstChunkOffset+firstWriteCount], remainingBuf)
			currentOffset += firstWriteCount
			remainingBuf = remainingBuf[firstWriteCount:]
			tx.Set(firstChunkKey, zeroTrimChunk(chunk))
		}

		if len(remainingBuf) > 0 {
			unalignedTrailingBytes := (currentOffset + uint64(len(remainingBuf))) % CHUNK_SIZE
			if unalignedTrailingBytes != 0 {
				// Do trailing unaligned bytes next write.
				remainingBuf = remainingBuf[:uint64(len(remainingBuf))-unalignedTrailingBytes]
			}
		}

		for len(remainingBuf) != 0 {
			key := tuple.Tuple{"fs", "ino", f.ino, "data", currentOffset / CHUNK_SIZE}
			tx.Set(key, zeroTrimChunk(remainingBuf[:CHUNK_SIZE]))
			currentOffset += CHUNK_SIZE
			remainingBuf = remainingBuf[CHUNK_SIZE:]
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
			stat.Size = offset + nWritten
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
func (f *foundationDBFile) ReadData(buf []byte, offset uint64) (uint32, error) {

	const MAX_READ = 128 * CHUNK_SIZE

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

		// Don't read past the end of the file.
		if stat.Size < currentOffset+uint64(len(remainingBuf)) {
			overshoot := (currentOffset + uint64(len(remainingBuf))) - stat.Size
			remainingBuf = remainingBuf[:uint64(len(remainingBuf))-overshoot]
			if len(remainingBuf) == 0 {
				return 0, io.EOF
			}
		}

		// Deal with the first unaligned and undersized chunk.
		if currentOffset%CHUNK_SIZE != 0 || len(remainingBuf) < CHUNK_SIZE {

			firstChunkNo := currentOffset / CHUNK_SIZE
			firstChunkOffset := currentOffset % CHUNK_SIZE
			firstReadCount := CHUNK_SIZE - firstChunkOffset
			if firstReadCount > uint64(len(remainingBuf)) {
				firstReadCount = uint64(len(remainingBuf))
			}

			firstChunkKey := tuple.Tuple{"fs", "ino", f.ino, "data", firstChunkNo}
			chunk := tx.Get(firstChunkKey).MustGet()
			if chunk != nil {
				zeroExpandChunk(&chunk)
				copy(remainingBuf[:firstReadCount], chunk[firstChunkOffset:firstChunkOffset+firstReadCount])
			} else {
				// Sparse read.
				for i := uint64(0); i < firstReadCount; i += 1 {
					remainingBuf[i] = 0
				}
			}
			remainingBuf = remainingBuf[firstReadCount:]
			currentOffset += firstReadCount
		}

		if len(remainingBuf) > 0 {
			unalignedTrailingBytes := (currentOffset + uint64(len(remainingBuf))) % CHUNK_SIZE
			if unalignedTrailingBytes != 0 {
				// Do trailing unaligned bytes next read.
				remainingBuf = remainingBuf[:uint64(len(remainingBuf))-unalignedTrailingBytes]
			}
		}

		nChunks := uint64(len(remainingBuf)) / CHUNK_SIZE
		chunkFutures := make([]fdb.FutureByteSlice, 0, nChunks)

		// Read all chunks in parallel using futures.
		for i := uint64(0); i < nChunks; i++ {
			key := tuple.Tuple{"fs", "ino", f.ino, "data", (currentOffset / CHUNK_SIZE) + i}
			chunkFutures = append(chunkFutures, tx.Get(key))
		}

		for i := uint64(0); i < nChunks; i++ {
			chunk := chunkFutures[i].MustGet()
			if chunk != nil {
				zeroExpandChunk(&chunk)
				copy(remainingBuf[:CHUNK_SIZE], chunk)
			} else {
				// Sparse read.
				for i := 0; i < CHUNK_SIZE; i++ {
					remainingBuf[i] = 0
				}
			}
			remainingBuf = remainingBuf[CHUNK_SIZE:]
			currentOffset += CHUNK_SIZE
		}

		nRead := currentOffset - offset

		if (offset + nRead) == stat.Size {
			return uint32(nRead), io.EOF
		}

		return uint32(nRead), nil
	})

	nReadInt, ok := nRead.(uint32)
	if ok {
		return nReadInt, err
	} else {
		return 0, err
	}
}
func (f *foundationDBFile) Fsync() error { return nil }
func (f *foundationDBFile) Flush() error { return nil }
func (f *foundationDBFile) Close() error { return nil }

type invalidFile struct{}

func (f *invalidFile) WriteData(buf []byte, offset uint64) (uint32, error) { return 0, ErrInvalid }
func (f *invalidFile) ReadData(buf []byte, offset uint64) (uint32, error)  { return 0, ErrInvalid }
func (f *invalidFile) Fsync() error                                        { return ErrInvalid }
func (f *invalidFile) Flush() error                                        { return nil }
func (f *invalidFile) Close() error                                        { return nil }

type externalStoreReadOnlyFile struct {
	storageObject readerAtCloser
}

func (f *externalStoreReadOnlyFile) WriteData(buf []byte, offset uint64) (uint32, error) {
	return 0, ErrNotSupported
}

func (f *externalStoreReadOnlyFile) ReadData(buf []byte, offset uint64) (uint32, error) {
	n, err := f.storageObject.ReadAt(buf, int64(offset))
	return uint32(n), err
}

func (f *externalStoreReadOnlyFile) Fsync() error {
	return nil
}

func (f *externalStoreReadOnlyFile) Flush() error {
	return nil
}

func (f *externalStoreReadOnlyFile) Close() error {
	return f.storageObject.Close()
}

type externalStoreReadWriteFile struct {
	fs        *Fs
	ino       uint64
	dirty     atomicBool
	storage   string
	flushLock sync.Mutex
	tmpFile   *os.File
}

func (f *externalStoreReadWriteFile) WriteData(buf []byte, offset uint64) (uint32, error) {
	f.dirty.Store(true)
	n, err := f.tmpFile.WriteAt(buf, int64(offset))
	return uint32(n), err
}

func (f *externalStoreReadWriteFile) ReadData(buf []byte, offset uint64) (uint32, error) {
	n, err := f.tmpFile.ReadAt(buf, int64(offset))
	return uint32(n), err
}

func (f *externalStoreReadWriteFile) Fsync() error {

	dirty := f.dirty.Load()
	if !dirty {
		return nil
	}

	f.flushLock.Lock()
	defer f.flushLock.Unlock()

	_, err := f.tmpFile.Seek(0, 0)
	if err != nil {
		return err
	}

	size, err := storageWrite(f.storage, f.ino, f.tmpFile)
	if err != nil {
		return err
	}

	_, err = f.fs.Transact(func(tx fdb.Transaction) (interface{}, error) {
		stat, err := f.fs.txGetStat(tx, f.ino).Get()
		if err != nil {
			return nil, err
		}
		stat.Size = uint64(size)
		f.fs.txSetStat(tx, stat)
		return nil, nil
	})
	if err != nil {
		return err
	}

	f.dirty.Store(false)
	return nil
}

func (f *externalStoreReadWriteFile) Flush() error {
	return f.Fsync()
}

func (f *externalStoreReadWriteFile) Close() error {
	_ = f.tmpFile.Close()
	return nil
}

type OpenFileOpts struct {
	Truncate bool
}

func (fs *Fs) OpenFile(ino uint64, opts OpenFileOpts) (HafsFile, Stat, error) {
	var stat Stat
	_, err := fs.Transact(func(tx fdb.Transaction) (interface{}, error) {

		existingStat, err := fs.txGetStat(tx, ino).Get()
		if err != nil {
			return nil, err
		}
		stat = existingStat

		if stat.Mode&S_IFMT != S_IFREG {
			return nil, ErrInvalid
		}

		if opts.Truncate {
			stat, err = fs.txModStat(tx, stat.Ino, ModStatOpts{
				Valid: MODSTAT_SIZE,
				Size:  0,
			})
			if err != nil {
				return nil, err
			}
		}

		return nil, nil
	})

	var f HafsFile
	if stat.Storage == "" {
		f = &foundationDBFile{
			fs:  fs,
			ino: stat.Ino,
		}
	} else {
		if opts.Truncate || stat.Size == 0 {
			tmpFile, err := os.CreateTemp("", "")
			if err != nil {
				return nil, Stat{}, err
			}
			// XXX Make file anonymous, it would be nice to create it like this.
			err = os.Remove(tmpFile.Name())
			if err != nil {
				return nil, Stat{}, err
			}
			f = &externalStoreReadWriteFile{
				fs:      fs,
				ino:     stat.Ino,
				storage: stat.Storage,
				tmpFile: tmpFile,
			}
		} else {
			storageObject, err := storageOpen(stat.Storage, stat.Ino)
			if err != nil {
				return nil, Stat{}, err
			}
			f = &externalStoreReadOnlyFile{
				storageObject: storageObject,
			}
		}
	}

	return f, stat, err
}

type CreateFileOpts struct {
	Truncate bool
	Mode     uint32
	Uid      uint32
	Gid      uint32
}

func (fs *Fs) CreateFile(dirIno uint64, name string, opts CreateFileOpts) (HafsFile, Stat, error) {
	var stat Stat
	_, err := fs.Transact(func(tx fdb.Transaction) (interface{}, error) {
		newStat, err := fs.Mknod(dirIno, name, MknodOpts{
			Truncate: opts.Truncate,
			Mode:     (^S_IFMT & opts.Mode) | S_IFREG,
			Uid:      opts.Uid,
			Gid:      opts.Gid,
		})
		if err != nil {
			return nil, err
		}
		stat = newStat
		return nil, nil
	})

	var f HafsFile
	if stat.Storage == "" {
		f = &foundationDBFile{
			fs:  fs,
			ino: stat.Ino,
		}
	} else {
		tmpFile, err := os.CreateTemp("", "")
		if err != nil {
			return nil, Stat{}, err
		}
		// XXX Make file anonymous, it would be nice to create it like this.
		err = os.Remove(tmpFile.Name())
		if err != nil {
			return nil, Stat{}, err
		}
		f = &externalStoreReadWriteFile{
			fs:      fs,
			ino:     stat.Ino,
			storage: stat.Storage,
			tmpFile: tmpFile,
		}
	}
	return f, stat, err
}

func (fs *Fs) ReadSymlink(ino uint64) ([]byte, error) {
	l, err := fs.Transact(func(tx fdb.Transaction) (interface{}, error) {
		statFut := fs.txGetStat(tx, ino)
		lFut := tx.Get(tuple.Tuple{"fs", "ino", ino, "target"})
		stat, err := statFut.Get()
		if err != nil {
			return nil, err
		}
		if stat.Mode&S_IFMT != S_IFLNK {
			return nil, ErrInvalid
		}
		return lFut.MustGet(), nil
	})
	if err != nil {
		return nil, err
	}
	return l.([]byte), nil
}

func (fs *Fs) GetStat(ino uint64) (Stat, error) {
	stat, err := fs.Transact(func(tx fdb.Transaction) (interface{}, error) {
		stat, err := fs.txGetStat(tx, ino).Get()
		return stat, err
	})
	if err != nil {
		return Stat{}, err
	}
	return stat.(Stat), nil
}

const (
	MODSTAT_MODE = 1 << iota
	MODSTAT_UID
	MODSTAT_GID
	MODSTAT_SIZE
	MODSTAT_ATIME
	MODSTAT_MTIME
	MODSTAT_CTIME
)

type ModStatOpts struct {
	Valid     uint32
	Size      uint64
	Atimesec  uint64
	Mtimesec  uint64
	Ctimesec  uint64
	Atimensec uint32
	Mtimensec uint32
	Ctimensec uint32
	Mode      uint32
	Uid       uint32
	Gid       uint32
}

func (opts *ModStatOpts) setTime(t time.Time, secs *uint64, nsecs *uint32) {
	*secs = uint64(t.UnixNano() / 1_000_000_000)
	*nsecs = uint32(t.UnixNano() % 1_000_000_000)
}

func (opts *ModStatOpts) SetMtime(t time.Time) {
	opts.Valid |= MODSTAT_MTIME
	opts.setTime(t, &opts.Mtimesec, &opts.Mtimensec)
}

func (opts *ModStatOpts) SetAtime(t time.Time) {
	opts.Valid |= MODSTAT_ATIME
	opts.setTime(t, &opts.Atimesec, &opts.Atimensec)
}

func (opts *ModStatOpts) SetCtime(t time.Time) {
	opts.Valid |= MODSTAT_CTIME
	opts.setTime(t, &opts.Ctimesec, &opts.Ctimensec)
}

func (opts *ModStatOpts) SetSize(size uint64) {
	opts.Valid |= MODSTAT_SIZE
	opts.Size = size
}

func (opts *ModStatOpts) SetMode(mode uint32) {
	opts.Valid |= MODSTAT_MODE
	opts.Mode = mode
}

func (opts *ModStatOpts) SetUid(uid uint32) {
	opts.Valid |= MODSTAT_UID
	opts.Uid = uid
}

func (opts *ModStatOpts) SetGid(gid uint32) {
	opts.Valid |= MODSTAT_GID
	opts.Gid = gid
}

func (fs *Fs) txModStat(tx fdb.Transaction, ino uint64, opts ModStatOpts) (Stat, error) {
	stat, err := fs.txGetStat(tx, ino).Get()
	if err != nil {
		return Stat{}, err
	}

	if opts.Valid&MODSTAT_MODE != 0 {
		stat.Mode = (stat.Mode & S_IFMT) | (opts.Mode & ^S_IFMT)
	}

	if opts.Valid&MODSTAT_UID != 0 {
		stat.Uid = opts.Uid
	}

	if opts.Valid&MODSTAT_GID != 0 {
		stat.Gid = opts.Gid
	}

	if opts.Valid&MODSTAT_ATIME != 0 {
		stat.Atimesec = opts.Atimesec
		stat.Atimensec = opts.Atimensec
	}

	now := time.Now()

	if opts.Valid&MODSTAT_MTIME != 0 {
		stat.Mtimesec = opts.Mtimesec
		stat.Mtimensec = opts.Mtimensec
	} else if opts.Valid&MODSTAT_SIZE != 0 {
		stat.SetMtime(now)
	}

	if opts.Valid&MODSTAT_CTIME != 0 {
		stat.Ctimesec = opts.Ctimesec
		stat.Ctimensec = opts.Ctimensec
	} else {
		stat.SetCtime(now)
	}

	if opts.Valid&MODSTAT_SIZE != 0 {
		stat.Size = opts.Size

		if stat.Mode&S_IFMT != S_IFREG {
			return Stat{}, ErrInvalid
		}

		if stat.Size == 0 {
			tx.ClearRange(tuple.Tuple{"fs", "ino", ino, "data"})
			/*
				if stat.Storage != "" {
					// XXX We have no way to reclaim the space for this object.
					// leaving it is relatively harmless as it will likely be overwritten.
					//
					// A good solution would be to add a generation count to object data and
					// queue the deletion of the old generation in this transaction.
				}
			*/
		} else {
			if stat.Storage != "" {
				return Stat{}, ErrNotSupported
			}

			clearBegin := (stat.Size + (CHUNK_SIZE - stat.Size%4096)) / CHUNK_SIZE
			_, clearEnd := tuple.Tuple{"fs", "ino", ino, "data"}.FDBRangeKeys()
			tx.ClearRange(fdb.KeyRange{
				Begin: tuple.Tuple{"fs", "ino", ino, "data", clearBegin},
				End:   clearEnd,
			})
			lastChunkIdx := stat.Size / CHUNK_SIZE
			lastChunkSize := stat.Size % CHUNK_SIZE
			lastChunkKey := tuple.Tuple{"fs", "ino", ino, "data", lastChunkIdx}
			if lastChunkSize == 0 {
				tx.Clear(lastChunkKey)
			}
		}
	}

	fs.txSetStat(tx, stat)
	return stat, nil
}

func (fs *Fs) ModStat(ino uint64, opts ModStatOpts) (Stat, error) {
	stat, err := fs.Transact(func(tx fdb.Transaction) (interface{}, error) {
		stat, err := fs.txModStat(tx, ino, opts)
		return stat, err
	})
	if err != nil {
		return Stat{}, err
	}
	return stat.(Stat), nil
}

func (fs *Fs) Lookup(dirIno uint64, name string) (Stat, error) {
	stat, err := fs.Transact(func(tx fdb.Transaction) (interface{}, error) {
		dirEnt, err := fs.txGetDirEnt(tx, dirIno, name).Get()
		if err != nil {
			return Stat{}, err
		}
		stat, err := fs.txGetStat(tx, dirEnt.Ino).Get()
		return stat, err
	})
	if err != nil {
		return Stat{}, err
	}
	return stat.(Stat), nil
}

func (fs *Fs) Rename(fromDirIno, toDirIno uint64, fromName, toName string) error {

	if fromName == toName && fromDirIno == toDirIno {
		return nil
	}

	_, err := fs.Transact(func(tx fdb.Transaction) (interface{}, error) {

		fromDirStatFut := fs.txGetStat(tx, fromDirIno)
		toDirStatFut := fromDirStatFut
		if toDirIno != fromDirIno {
			toDirStatFut = fs.txGetStat(tx, toDirIno)
		}
		fromDirEntFut := fs.txGetDirEnt(tx, fromDirIno, fromName)
		toDirEntFut := fs.txGetDirEnt(tx, toDirIno, toName)

		fromDirStat, fromDirStatErr := fromDirStatFut.Get()
		toDirStat, toDirStatErr := toDirStatFut.Get()
		fromDirEnt, fromDirEntErr := fromDirEntFut.Get()
		toDirEnt, toDirEntErr := toDirEntFut.Get()

		if toDirStatErr != nil {
			return nil, toDirStatErr
		}

		if toDirStat.Mode&S_IFMT != S_IFDIR {
			return nil, ErrNotDir
		}

		if fromDirStatErr != nil {
			return nil, fromDirStatErr
		}

		if fromDirStat.Mode&S_IFMT != S_IFDIR {
			return nil, ErrNotDir
		}

		if fromDirEntErr != nil {
			return nil, fromDirEntErr
		}

		now := time.Now()

		if errors.Is(toDirEntErr, ErrNotExist) {
			/* Nothing to do. */
		} else if toDirEntErr != nil {
			return nil, toDirEntErr
		} else {
			toStat, err := fs.txGetStat(tx, toDirEnt.Ino).Get()
			if err != nil {
				return nil, err
			}

			if toStat.Mode&S_IFMT == S_IFDIR {
				if fs.txDirHasChildren(tx, toStat.Ino) {
					return nil, ErrNotEmpty
				}
			}

			toStat.Nlink -= 1
			toStat.SetMtime(now)
			toStat.SetCtime(now)
			fs.txSetStat(tx, toStat)

			if toStat.Nlink == 0 {
				tx.Set(tuple.Tuple{"fs", "unlinked", toStat.Ino}, []byte{})
			}
		}

		if toDirIno != fromDirIno {
			toDirStat.SetMtime(now)
			toDirStat.SetCtime(now)
			fs.txSetStat(tx, toDirStat)
			fromDirStat.SetMtime(now)
			toDirStat.SetCtime(now)
			fs.txSetStat(tx, fromDirStat)
		} else {
			toDirStat.SetMtime(now)
			toDirStat.SetCtime(now)
			fs.txSetStat(tx, toDirStat)
		}

		tx.Clear(tuple.Tuple{"fs", "ino", fromDirIno, "child", fromName})
		fs.txSetDirEnt(tx, toDirIno, DirEnt{
			Name: toName,
			Mode: fromDirEnt.Mode,
			Ino:  fromDirEnt.Ino,
		})
		return nil, nil
	})
	return err
}

type DirIter struct {
	lock      sync.Mutex
	fs        *Fs
	iterRange fdb.KeyRange
	ents      []DirEnt
	done      bool
}

func (di *DirIter) fill() error {
	const BATCH_SIZE = 128

	v, err := di.fs.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
		// XXX should we confirm the directory still exists?
		kvs := tx.GetRange(di.iterRange, fdb.RangeOptions{
			Limit: BATCH_SIZE,
		}).GetSliceOrPanic()
		return kvs, nil
	})
	if err != nil {
		return err
	}

	kvs := v.([]fdb.KeyValue)

	if len(kvs) != 0 {
		nextBegin, err := fdb.Strinc(kvs[len(kvs)-1].Key)
		if err != nil {
			return err
		}
		di.iterRange.Begin = fdb.Key(nextBegin)
	} else {
		di.iterRange.Begin = di.iterRange.End
	}

	ents := make([]DirEnt, 0, len(kvs))

	for _, kv := range kvs {
		keyTuple, err := tuple.Unpack(kv.Key)
		if err != nil {
			return err
		}
		name := keyTuple[len(keyTuple)-1].(string)
		dirEnt := DirEnt{}
		err = json.Unmarshal(kv.Value, &dirEnt)
		if err != nil {
			return err
		}
		dirEnt.Name = name
		ents = append(ents, dirEnt)
	}

	// Reverse entries so we can pop them off in the right order.
	for i, j := 0, len(ents)-1; i < j; i, j = i+1, j-1 {
		ents[i], ents[j] = ents[j], ents[i]
	}

	if len(ents) < BATCH_SIZE {
		di.done = true
	}

	di.ents = ents

	return nil
}

func (di *DirIter) Next() (DirEnt, error) {
	di.lock.Lock()
	defer di.lock.Unlock()

	if len(di.ents) == 0 && di.done {
		return DirEnt{}, io.EOF
	}

	// Fill initial listing, otherwise we should always have something.
	if len(di.ents) == 0 {
		err := di.fill()
		if err != nil {
			return DirEnt{}, err
		}
		if len(di.ents) == 0 && di.done {
			return DirEnt{}, io.EOF
		}
	}

	nextEnt := di.ents[len(di.ents)-1]
	di.ents = di.ents[:len(di.ents)-1]
	return nextEnt, nil
}

func (di *DirIter) Unget(ent DirEnt) {
	di.lock.Lock()
	defer di.lock.Unlock()
	di.ents = append(di.ents, ent)
	di.done = false
}

func (fs *Fs) IterDirEnts(dirIno uint64) (*DirIter, error) {
	iterBegin, iterEnd := tuple.Tuple{"fs", "ino", dirIno, "child"}.FDBRangeKeys()
	di := &DirIter{
		fs: fs,
		iterRange: fdb.KeyRange{
			Begin: iterBegin,
			End:   iterEnd,
		},
		ents: []DirEnt{},
		done: false,
	}
	err := di.fill()
	return di, err
}

func (fs *Fs) GetXAttr(ino uint64, name string) ([]byte, error) {
	x, err := fs.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
		statFut := fs.txGetStat(tx, ino)
		xFut := tx.Get(tuple.Tuple{"fs", "ino", ino, "xattr", name})
		_, err := statFut.Get()
		if err != nil {
			return nil, err
		}
		return xFut.MustGet(), nil
	})
	if err != nil {
		return nil, err
	}
	return x.([]byte), nil
}

func (fs *Fs) SetXAttr(ino uint64, name string, data []byte) error {
	_, err := fs.Transact(func(tx fdb.Transaction) (interface{}, error) {
		stat, err := fs.txGetStat(tx, ino).Get()
		if err != nil {
			return nil, err
		}
		switch name {
		case "hafs.storage":
			// You can only set this attribute on directories.
			if stat.Mode&S_IFMT != S_IFDIR {
				return nil, ErrInvalid
			}
			stat.Storage = string(data)
			fs.txSetStat(tx, stat)
		default:
		}
		tx.Set(tuple.Tuple{"fs", "ino", ino, "xattr", name}, data)
		return nil, nil
	})
	return err
}

func (fs *Fs) RemoveXAttr(ino uint64, name string) error {
	_, err := fs.Transact(func(tx fdb.Transaction) (interface{}, error) {
		stat, err := fs.txGetStat(tx, ino).Get()
		if err != nil {
			return nil, err
		}
		switch name {
		case "hafs.storage":
			// You cannot clear this attribute from files.
			if stat.Mode&S_IFMT == S_IFREG {
				return nil, ErrInvalid
			}
			stat.Storage = ""
			fs.txSetStat(tx, stat)
		default:
		}
		tx.Clear(tuple.Tuple{"fs", "ino", ino, "xattr", name})
		return nil, nil
	})
	return err
}

func (fs *Fs) ListXAttr(ino uint64) ([]string, error) {
	v, err := fs.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
		_, err := fs.txGetStat(tx, ino).Get()
		if err != nil {
			return nil, err
		}
		kvs := tx.GetRange(tuple.Tuple{"fs", "ino", ino, "xattr"}, fdb.RangeOptions{}).GetSliceOrPanic()
		return kvs, nil
	})
	if err != nil {
		return nil, err
	}
	kvs := v.([]fdb.KeyValue)
	xattrs := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		unpacked, err := tuple.Unpack(kv.Key)
		if err != nil {
			return nil, err
		}
		xattrs = append(xattrs, unpacked[len(unpacked)-1].(string))
	}
	return xattrs, nil
}

const (
	LOCK_NONE = iota
	LOCK_SHARED
	LOCK_EXCLUSIVE
)

type LockType uint32

type SetLockOpts struct {
	Typ   LockType
	Owner uint64
}

type exclusiveLockRecord struct {
	ClientId string
	Owner    uint64
}

func (fs *Fs) TrySetLock(ino uint64, opts SetLockOpts) (bool, error) {
	ok, err := fs.Transact(func(tx fdb.Transaction) (interface{}, error) {
		stat, err := fs.txGetStat(tx, ino).Get()
		if err != nil {
			return false, err
		}

		if stat.Mode&S_IFMT != S_IFREG {
			return false, ErrInvalid
		}

		exclusiveLockKey := tuple.Tuple{"fs", "ino", ino, "lock", "exclusive"}

		switch opts.Typ {
		case LOCK_NONE:
			exclusiveLockBytes := tx.Get(exclusiveLockKey).MustGet()
			if exclusiveLockBytes != nil {
				exclusiveLock := exclusiveLockRecord{}
				err := json.Unmarshal(exclusiveLockBytes, &exclusiveLock)
				if err != nil {
					return false, err
				}
				// The lock isn't owned by this client.
				if exclusiveLock.ClientId != fs.mountId {
					return false, nil
				}
				// The request isn't for this owner.
				if exclusiveLock.Owner != opts.Owner {
					return false, nil
				}
				tx.Clear(exclusiveLockKey)
			} else {
				sharedLockKey := tuple.Tuple{"fs", "ino", ino, "lock", "shared", fs.mountId, opts.Owner}
				tx.Clear(sharedLockKey)
			}
			tx.Clear(tuple.Tuple{"fs", "mount", fs.mountId, "lock", ino, opts.Owner})
			return true, nil
		case LOCK_SHARED:
			exclusiveLockBytes := tx.Get(exclusiveLockKey).MustGet()
			if exclusiveLockBytes != nil {
				return false, nil
			}
			tx.Set(tuple.Tuple{"fs", "ino", ino, "lock", "shared", fs.mountId, opts.Owner}, []byte{})
			tx.Set(tuple.Tuple{"fs", "mount", fs.mountId, "lock", ino, opts.Owner}, []byte{})
			return true, nil
		case LOCK_EXCLUSIVE:
			exclusiveLockBytes := tx.Get(exclusiveLockKey).MustGet()
			if exclusiveLockBytes != nil {
				return false, nil
			}
			sharedLocks := tx.GetRange(tuple.Tuple{"fs", "ino", ino, "lock", "shared"}, fdb.RangeOptions{
				Limit: 1,
			}).GetSliceOrPanic()
			if len(sharedLocks) > 0 {
				return false, nil
			}
			exclusiveLockBytes, err := json.Marshal(exclusiveLockRecord{
				ClientId: fs.mountId,
				Owner:    opts.Owner,
			})
			if err != nil {
				return false, err
			}
			tx.Set(exclusiveLockKey, exclusiveLockBytes)
			tx.Set(tuple.Tuple{"fs", "mount", fs.mountId, "lock", ino, opts.Owner}, []byte{})
			return true, nil
		default:
			panic("api misuse")
		}
	})
	if err != nil {
		return false, nil
	}
	return ok.(bool), nil
}

func (fs *Fs) PollAwaitExclusiveLockRelease(cancel <-chan struct{}, ino uint64) error {
	for {
		released, err := fs.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
			exclusiveLockKey := tuple.Tuple{"fs", "ino", ino, "lock", "exclusive"}
			return tx.Get(exclusiveLockKey).MustGet() == nil, nil
		})
		if err != nil {
			return err
		}
		if released.(bool) == true {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
}

func (fs *Fs) AwaitExclusiveLockRelease(cancel <-chan struct{}, ino uint64) error {
	w, err := fs.Transact(func(tx fdb.Transaction) (interface{}, error) {
		exclusiveLockKey := tuple.Tuple{"fs", "ino", ino, "lock", "exclusive"}
		if tx.Get(exclusiveLockKey).MustGet() == nil {
			return nil, nil
		}
		w := tx.Watch(exclusiveLockKey)
		return w, nil
	})
	if err != nil {
		return fs.PollAwaitExclusiveLockRelease(cancel, ino)
	}
	if w == nil {
		return nil
	}

	watch := w.(fdb.FutureNil)
	result := make(chan error, 1)
	go func() {
		result <- watch.Get()
	}()

	select {
	case <-cancel:
		watch.Cancel()
		return ErrIntr
	case err := <-result:
		return err
	}
}

func (fs *Fs) RemoveExpiredUnlinked(removalDelay time.Duration) (uint64, error) {

	iterBegin, iterEnd := tuple.Tuple{"fs", "unlinked"}.FDBRangeKeys()

	iterRange := fdb.KeyRange{
		Begin: iterBegin,
		End:   iterEnd,
	}

	nRemoved := uint64(0)

	errg, _ := errgroup.WithContext(context.Background())
	errg.SetLimit(128)

	for {

		v, err := fs.Transact(func(tx fdb.Transaction) (interface{}, error) {
			kvs := tx.GetRange(iterRange, fdb.RangeOptions{
				Limit: 128,
			}).GetSliceOrPanic()
			return kvs, nil
		})
		if err != nil {
			return nRemoved, err
		}

		kvs := v.([]fdb.KeyValue)

		if len(kvs) == 0 {
			break
		}

		nextBegin, err := fdb.Strinc(kvs[len(kvs)-1].Key)
		if err != nil {
			return nRemoved, err
		}
		iterRange.Begin = fdb.Key(nextBegin)

		errg.Go(func() error {

			v, err = fs.Transact(func(tx fdb.Transaction) (interface{}, error) {

				futureStats := make([]futureStat, 0, len(kvs))
				for _, kv := range kvs {
					keyTuple, err := tuple.Unpack(kv.Key)
					if err != nil {
						return nil, err
					}
					ino := uint64(keyTuple[len(keyTuple)-1].(int64))
					futureStats = append(futureStats, fs.txGetStat(tx, ino))
				}

				expiredStats := make([]Stat, 0, len(futureStats))

				now := time.Now()
				for _, futureStat := range futureStats {
					stat, err := futureStat.Get()
					if err != nil {
						return nil, err
					}
					if errors.Is(err, ErrNotExist) {
						continue
					}
					if now.After(stat.Ctime().Add(removalDelay)) {
						expiredStats = append(expiredStats, stat)
					}
				}

				return expiredStats, nil
			})
			if err != nil {
				return err
			}

			expiredStats := v.([]Stat)

			if len(expiredStats) == 0 {
				return nil
			}

			for _, stat := range expiredStats {
				if stat.Mode&S_IFMT != S_IFREG {
					continue
				}

				if stat.Storage != "" {
					err := storageRemove(stat.Storage, stat.Ino)
					if err != nil {
						return err
					}
				}
			}

			_, err = fs.Transact(func(tx fdb.Transaction) (interface{}, error) {
				for _, stat := range expiredStats {
					tx.Clear(tuple.Tuple{"fs", "unlinked", stat.Ino})
					tx.ClearRange(tuple.Tuple{"fs", "ino", stat.Ino})
				}
				return nil, nil
			})
			if err != nil {
				return err
			}

			atomic.AddUint64(&nRemoved, uint64(len(expiredStats)))
			return nil

		})

	}

	err := errg.Wait()
	if err != nil {
		return nRemoved, err
	}

	return nRemoved, nil
}

func (fs *Fs) txBreakLock(tx fdb.Transaction, clientId string, ino uint64, owner uint64) error {
	exclusiveLockKey := tuple.Tuple{"fs", "ino", ino, "lock", "exclusive"}
	exclusiveLockBytes := tx.Get(exclusiveLockKey).MustGet()
	if exclusiveLockBytes != nil {
		exclusiveLock := exclusiveLockRecord{}
		err := json.Unmarshal(exclusiveLockBytes, &exclusiveLock)
		if err != nil {
			return err
		}
		if exclusiveLock.ClientId == fs.mountId && exclusiveLock.Owner == owner {
			tx.Clear(exclusiveLockKey)
		}
	} else {
		sharedLockKey := tuple.Tuple{"fs", "ino", ino, "lock", "shared", clientId, owner}
		tx.Clear(sharedLockKey)
	}
	tx.Clear(tuple.Tuple{"fs", "mount", clientId, "lock", ino, owner})
	return nil
}

func (fs *Fs) EvictClient(clientId string) error {

	_, err := fs.db.Transact(func(tx fdb.Transaction) (interface{}, error) {
		// Invalidate all the clients in progress transactions.
		tx.Clear(tuple.Tuple{"fs", "mount", clientId, "attached"})
		return nil, nil
	})
	if err != nil {
		return err
	}

	// Remove all file locks held by the client.
	iterBegin, iterEnd := tuple.Tuple{"fs", "mount", clientId, "lock"}.FDBRangeKeys()

	iterRange := fdb.KeyRange{
		Begin: iterBegin,
		End:   iterEnd,
	}

	for {
		v, err := fs.db.Transact(func(tx fdb.Transaction) (interface{}, error) {
			kvs := tx.GetRange(iterRange, fdb.RangeOptions{
				Limit: 64,
			}).GetSliceOrPanic()
			return kvs, nil
		})
		if err != nil {
			return err
		}

		kvs := v.([]fdb.KeyValue)

		if len(kvs) == 0 {
			break
		}

		nextBegin, err := fdb.Strinc(kvs[len(kvs)-1].Key)
		if err != nil {
			return err
		}
		iterRange.Begin = fdb.Key(nextBegin)

		_, err = fs.db.Transact(func(tx fdb.Transaction) (interface{}, error) {
			for _, kv := range kvs {
				tup, err := tuple.Unpack(kv.Key)
				if err != nil {
					return nil, err
				}
				if len(tup) < 2 {
					return nil, errors.New("corrupt lock entry")
				}
				owner := uint64(tup[len(tup)-1].(int64))
				ino := uint64(tup[len(tup)-2].(int64))
				err = fs.txBreakLock(tx, clientId, ino, owner)
				if err != nil {
					return nil, err
				}
			}

			return nil, nil
		})

		if err != nil {
			return err
		}

	}

	// Finally we can remove the client.
	_, err = fs.db.Transact(func(tx fdb.Transaction) (interface{}, error) {
		tx.Clear(tuple.Tuple{"fs", "mounts", clientId})
		tx.ClearRange(tuple.Tuple{"fs", "mount", clientId})
		return nil, nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (fs *Fs) IsClientTimedOut(clientId string, clientTimeout time.Duration) (bool, error) {
	timedOut, err := fs.Transact(func(tx fdb.Transaction) (interface{}, error) {
		var heartBeat uint64
		heatBeatKey := tuple.Tuple{"fs", "mount", clientId, "heartbeat"}
		heartBeatBytes := tx.Get(heatBeatKey).MustGet()
		if heartBeatBytes == nil {
			return true, nil
		}
		err := json.Unmarshal(heartBeatBytes, &heartBeat)
		if err != nil {
			return nil, err
		}
		lastSeen := time.Unix(int64(heartBeat), 0)
		timedOut := lastSeen.Add(clientTimeout).Before(time.Now())
		return timedOut, nil
	})

	if err != nil {
		return false, err
	}

	return timedOut.(bool), nil
}

func (fs *Fs) RemoveExpiredClients(clientTimeout time.Duration) (uint64, error) {

	nEvicted := uint64(0)

	iterBegin, iterEnd := tuple.Tuple{"fs", "mounts"}.FDBRangeKeys()

	iterRange := fdb.KeyRange{
		Begin: iterBegin,
		End:   iterEnd,
	}

	for {
		v, err := fs.Transact(func(tx fdb.Transaction) (interface{}, error) {
			kvs := tx.GetRange(iterRange, fdb.RangeOptions{
				Limit: 100,
			}).GetSliceOrPanic()
			return kvs, nil
		})
		if err != nil {
			return nEvicted, err
		}

		kvs := v.([]fdb.KeyValue)

		if len(kvs) == 0 {
			break
		}

		nextBegin, err := fdb.Strinc(kvs[len(kvs)-1].Key)
		if err != nil {
			return nEvicted, err
		}
		iterRange.Begin = fdb.Key(nextBegin)

		for _, kv := range kvs {
			tup, err := tuple.Unpack(kv.Key)
			if err != nil {
				return nEvicted, err
			}

			if len(tup) < 1 {
				return nEvicted, errors.New("corrupt client key")
			}

			clientId := tup[len(tup)-1].(string)

			shouldEvict, err := fs.IsClientTimedOut(clientId, clientTimeout)
			if err != nil {
				return nEvicted, err
			}

			if !shouldEvict {
				continue
			}

			err = fs.EvictClient(clientId)
			if err != nil {
				return nEvicted, err
			}

			nEvicted += 1
		}
	}

	return nEvicted, nil
}

type CollectGarbageOpts struct {
	UnlinkedRemovalDelay time.Duration
	ClientTimeout        time.Duration
}

type CollectGarbageStats struct {
	UnlinkedRemovalCount uint64
	ClientEvictionCount  uint64
}

func (fs *Fs) CollectGarbage(opts CollectGarbageOpts) (CollectGarbageStats, error) {
	var err error
	stats := CollectGarbageStats{}

	stats.UnlinkedRemovalCount, err = fs.RemoveExpiredUnlinked(opts.UnlinkedRemovalDelay)
	if err != nil {
		return stats, err
	}

	stats.ClientEvictionCount, err = fs.RemoveExpiredClients(opts.ClientTimeout)
	if err != nil {
		return stats, err
	}

	return stats, nil
}

type ClientInfo struct {
	Id             string `json:",omitempty"`
	Description    string
	Hostname       string
	Pid            int64
	Exe            string
	AttachTimeUnix uint64    `json:",omitempty"`
	AttachTime     time.Time `json:"-"`
	HeartBeatUnix  uint64    `json:",omitempty"`
	HeartBeat      time.Time `json:"-"`
}

func (fs *Fs) ClientInfo(clientId string) (ClientInfo, bool, error) {

	var ok bool
	var info ClientInfo

	_, err := fs.Transact(func(tx fdb.Transaction) (interface{}, error) {
		info = ClientInfo{}
		ok = false

		infoBytes := tx.Get(tuple.Tuple{"fs", "mount", clientId, "info"}).MustGet()
		if infoBytes == nil {
			return nil, nil
		}

		err := json.Unmarshal(infoBytes, &info)
		if err != nil {
			return nil, err
		}

		heartBeatBytes := tx.Get(tuple.Tuple{"fs", "mount", clientId, "heartbeat"}).MustGet()
		err = json.Unmarshal(heartBeatBytes, &info.HeartBeatUnix)
		if err != nil {
			return nil, err
		}
		info.Id = clientId
		ok = true
		return nil, nil
	})

	info.HeartBeat = time.Unix(int64(info.HeartBeatUnix), 0)
	info.AttachTime = time.Unix(int64(info.AttachTimeUnix), 0)
	return info, ok, err
}

func (fs *Fs) ListClients() ([]ClientInfo, error) {

	clients := []ClientInfo{}

	iterBegin, iterEnd := tuple.Tuple{"fs", "mounts"}.FDBRangeKeys()

	iterRange := fdb.KeyRange{
		Begin: iterBegin,
		End:   iterEnd,
	}

	for {
		v, err := fs.Transact(func(tx fdb.Transaction) (interface{}, error) {
			kvs := tx.GetRange(iterRange, fdb.RangeOptions{
				Limit: 100,
			}).GetSliceOrPanic()
			return kvs, nil
		})
		if err != nil {
			return clients, err
		}

		kvs := v.([]fdb.KeyValue)

		if len(kvs) == 0 {
			break
		}

		nextBegin, err := fdb.Strinc(kvs[len(kvs)-1].Key)
		if err != nil {
			return clients, err
		}
		iterRange.Begin = fdb.Key(nextBegin)

		for _, kv := range kvs {
			tup, err := tuple.Unpack(kv.Key)
			if err != nil {
				return clients, err
			}

			if len(tup) < 1 {
				return clients, errors.New("corrupt client key")
			}

			clientId := tup[len(tup)-1].(string)

			client, ok, err := fs.ClientInfo(clientId)
			if err != nil {
				return clients, err
			}
			if ok {
				clients = append(clients, client)
			}
		}
	}

	return clients, nil
}
