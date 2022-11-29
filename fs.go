package hafs

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/bits"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/detailyang/fastrand-go"
	"github.com/valyala/fastjson"
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
	ErrNameTooLong  = unix.ENAMETOOLONG
	ErrDetached     = errors.New("filesystem detached")
)

const (
	NAME_MAX               = 4096
	CURRENT_SCHEMA_VERSION = 1
	ROOT_INO               = 1
	CHUNK_SIZE             = 4096
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

const (
	FLAG_SUBVOLUME uint64 = 1 << iota
	FLAG_EXTERNAL_STORAGE
)

type DirEnt struct {
	Name string
	Mode uint32
	Ino  uint64
}

func (e *DirEnt) MarshalBinary() ([]byte, error) {
	if S_IFMT != 0xf000 {
		// This check should be removed by the compiler.
		panic("encoding assumption violated")
	}
	bufsz := 2 * binary.MaxVarintLen64
	buf := make([]byte, bufsz, bufsz)
	b := buf
	b = b[binary.PutUvarint(b, uint64(e.Mode)>>12):]
	b = b[binary.PutUvarint(b, e.Ino):]
	return buf[:len(buf)-len(b)], nil
}

func (e *DirEnt) UnmarshalBinary(buf []byte) error {
	r := bytes.NewReader(buf)
	mode, _ := binary.ReadUvarint(r)
	e.Mode = uint32(mode << 12)
	e.Ino, _ = binary.ReadUvarint(r)
	return nil
}

type Stat struct {
	Ino       uint64
	Subvolume uint64
	Flags     uint64
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
	Storage   string
}

func (s *Stat) MarshalBinary() ([]byte, error) {
	bufsz := 15*binary.MaxVarintLen64 + len(s.Storage)
	buf := make([]byte, bufsz, bufsz)
	b := buf
	b = b[binary.PutUvarint(b, s.Subvolume):]
	b = b[binary.PutUvarint(b, s.Flags):]
	b = b[binary.PutUvarint(b, s.Size):]
	b = b[binary.PutUvarint(b, s.Atimesec):]
	b = b[binary.PutUvarint(b, s.Mtimesec):]
	b = b[binary.PutUvarint(b, s.Ctimesec):]
	b = b[binary.PutUvarint(b, uint64(s.Atimensec)):]
	b = b[binary.PutUvarint(b, uint64(s.Mtimensec)):]
	b = b[binary.PutUvarint(b, uint64(s.Ctimensec)):]
	b = b[binary.PutUvarint(b, uint64(s.Mode)):]
	b = b[binary.PutUvarint(b, uint64(s.Nlink)):]
	b = b[binary.PutUvarint(b, uint64(s.Uid)):]
	b = b[binary.PutUvarint(b, uint64(s.Gid)):]
	b = b[binary.PutUvarint(b, uint64(s.Rdev)):]
	b = b[binary.PutUvarint(b, uint64(len(s.Storage))):]
	b = b[copy(b, s.Storage):]
	return buf[:len(buf)-len(b)], nil
}

func (s *Stat) UnmarshalBinary(buf []byte) error {
	r := bytes.NewReader(buf)
	s.Subvolume, _ = binary.ReadUvarint(r)
	s.Flags, _ = binary.ReadUvarint(r)
	s.Size, _ = binary.ReadUvarint(r)
	s.Atimesec, _ = binary.ReadUvarint(r)
	s.Mtimesec, _ = binary.ReadUvarint(r)
	s.Ctimesec, _ = binary.ReadUvarint(r)
	v, _ := binary.ReadUvarint(r)
	s.Atimensec = uint32(v)
	v, _ = binary.ReadUvarint(r)
	s.Mtimensec = uint32(v)
	v, _ = binary.ReadUvarint(r)
	s.Ctimensec = uint32(v)
	v, _ = binary.ReadUvarint(r)
	s.Mode = uint32(v)
	v, _ = binary.ReadUvarint(r)
	s.Nlink = uint32(v)
	v, _ = binary.ReadUvarint(r)
	s.Uid = uint32(v)
	v, _ = binary.ReadUvarint(r)
	s.Gid = uint32(v)
	v, _ = binary.ReadUvarint(r)
	s.Rdev = uint32(v)
	v, _ = binary.ReadUvarint(r)
	var sb strings.Builder
	sb.Grow(int(v))
	io.CopyN(&sb, r, int64(v))
	s.Storage = sb.String()
	return nil
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
	db             fdb.Database
	fsName         string
	clientId       string
	onEviction     func(fs *Fs)
	clientDetached atomicBool
	txCounter      atomicUint64
	inoChan        chan uint64
	relMtime       time.Duration
	objectStorage  *ObjectStorageEngineCache

	workerWg      *sync.WaitGroup
	cancelWorkers func()
	logf          func(string, ...interface{})
}

func ListFilesystems(db fdb.Database) ([]string, error) {

	filesystems := []string{}

	iterBegin, iterEnd := tuple.Tuple{"hafs"}.FDBRangeKeys()
	iterRange := fdb.KeyRange{
		Begin: iterBegin,
		End:   iterEnd,
	}

	v, err := db.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
		kvs := tx.GetRange(iterRange, fdb.RangeOptions{
			Limit: 1,
		}).GetSliceOrPanic()
		return kvs, nil
	})
	if err != nil {
		return nil, err
	}
	kvs := v.([]fdb.KeyValue)
	if len(kvs) == 0 {
		return filesystems, nil
	}

	curFsTup, err := tuple.Unpack(kvs[0].Key)
	if err != nil {
		return nil, err
	}

	filesystems = append(filesystems, curFsTup[1].(string))

	for {
		_, iterRange.Begin = curFsTup[:2].FDBRangeKeys()

		v, err := db.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
			kvs := tx.GetRange(iterRange, fdb.RangeOptions{
				Limit: 1,
			}).GetSliceOrPanic()
			return kvs, nil
		})
		if err != nil {
			return nil, err
		}
		kvs := v.([]fdb.KeyValue)
		if len(kvs) == 0 {
			return filesystems, nil
		}

		curFsTup, err = tuple.Unpack(kvs[0].Key)
		if err != nil {
			return nil, err
		}

		filesystems = append(filesystems, curFsTup[1].(string))
	}

}

type AttachOpts struct {
	ClientDescription        string
	OnEviction               func(fs *Fs)
	Logf                     func(string, ...interface{})
	RelMtime                 *time.Duration
	ObjectStorageEngineCache *ObjectStorageEngineCache
}

func Attach(db fdb.Database, fsName string, opts AttachOpts) (*Fs, error) {

	if opts.ObjectStorageEngineCache == nil {
		opts.ObjectStorageEngineCache = &DefaultObjectStorageEngineCache
	}

	if opts.RelMtime == nil {
		defaultRelMtime := 24 * time.Hour
		opts.RelMtime = &defaultRelMtime
	}

	if opts.Logf == nil {
		opts.Logf = log.Printf
	}

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
	_, err := cryptorand.Read(idBytes[:])
	if err != nil {
		return nil, err
	}
	clientId := hex.EncodeToString(idBytes[:])

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

	initialHeartBeatBytes := [8]byte{}
	binary.LittleEndian.PutUint64(initialHeartBeatBytes[:], uint64(now.Unix()))

	_, err = db.Transact(func(tx fdb.Transaction) (interface{}, error) {
		version := tx.Get(tuple.Tuple{"hafs", fsName, "version"}).MustGet()
		if version == nil {
			return nil, errors.New("filesystem is not formatted")
		}
		if !bytes.Equal(version, []byte{CURRENT_SCHEMA_VERSION}) {
			return nil, fmt.Errorf("filesystem has different version - expected %d but got %d", CURRENT_SCHEMA_VERSION, version[0])
		}

		tx.Set(tuple.Tuple{"hafs", fsName, "client", clientId, "info"}, clientInfoBytes)
		tx.Set(tuple.Tuple{"hafs", fsName, "client", clientId, "heartbeat"}, initialHeartBeatBytes[:])
		tx.Set(tuple.Tuple{"hafs", fsName, "client", clientId, "attached"}, []byte{})
		tx.Set(tuple.Tuple{"hafs", fsName, "clients", clientId}, []byte{})
		return nil, nil
	})
	if err != nil {
		return nil, fmt.Errorf("unable to add mount: %w", err)
	}

	workerCtx, cancelWorkers := context.WithCancel(context.Background())

	fs := &Fs{
		db:            db,
		fsName:        fsName,
		onEviction:    opts.OnEviction,
		logf:          opts.Logf,
		relMtime:      *opts.RelMtime,
		clientId:      clientId,
		cancelWorkers: cancelWorkers,
		objectStorage: opts.ObjectStorageEngineCache,
		workerWg:      &sync.WaitGroup{},
		inoChan:       make(chan uint64, _INO_CHAN_SIZE),
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
	return ino == FUSE_UNKNOWN_INO || ino == RESERVED_INO_1 || ino == ROOT_INO || ino == 0
}

func (fs *Fs) nextIno() (uint64, error) {
	// XXX This is a single bottleneck, could we shard or use atomics?
	for {
		ino, ok := <-fs.inoChan
		if !ok {
			return 0, ErrDetached
		}
		if !reservedIno(ino) {
			return ino, nil
		}
	}
}

// We try to allocate inodes in an order that helps foundationDB distribute
// writes. We do this by stepping over the key space in fairly large
// chunks and allocating inodes in their reverse bit order, this hopefully
// lets inodes be written to servers in a nice spread over the keyspace.
const (
	_INO_STEP      = 100271        // Large Prime, chosen so that ~100 bytes per stat * _INO_STEP covers a foundationdb range quickly to distribute load.
	_INO_CHAN_SIZE = _INO_STEP - 1 // The channel is big enough so the first step blocks until one it taken.
)

func (fs *Fs) requestInosForever(ctx context.Context) {
	// Start with a small batch size, many clients  never allocate an inode.
	inoBatchSize := uint64(_INO_STEP)
	inoCounterKey := tuple.Tuple{"hafs", fs.fsName, "inocntr"}
	defer close(fs.inoChan)
	for {
		v, err := fs.Transact(func(tx fdb.Transaction) (interface{}, error) {
			inoCounterBytes := tx.Get(inoCounterKey).MustGet()
			if len(inoCounterBytes) != 8 {
				panic("corrupt inode counter")
			}
			currentCount := binary.LittleEndian.Uint64(inoCounterBytes)
			if (currentCount % _INO_STEP) != 0 {
				// Realign the count with _INO_STEP if it has become out of sync.
				currentCount += _INO_STEP - (currentCount % _INO_STEP)
			}
			nextInoCount := currentCount + inoBatchSize
			if nextInoCount >= 0x7FFFFFFFFFFFFFFF {
				// Avoid overflow and other strange cases.
				panic("inodes exhausted")
			}
			binary.LittleEndian.PutUint64(inoCounterBytes, nextInoCount)
			tx.Set(inoCounterKey, inoCounterBytes)
			return currentCount, nil
		})
		if err != nil {
			if errors.Is(err, ErrDetached) {
				return
			}
			fs.logf("unable to allocate inode batch: %s", err)
			time.Sleep(1 * time.Second)
			continue
		}
		inoBatchStart := v.(uint64)

		for i := uint64(0); i < _INO_STEP; i++ {
			for j := inoBatchStart; j < inoBatchStart+inoBatchSize; j += _INO_STEP {
				// Reverse the bits of the counter for better write load balancing.
				ino := bits.Reverse64(j + i)
				select {
				case fs.inoChan <- ino:
				default:
					select {
					case fs.inoChan <- ino:
					case <-ctx.Done():
						return
					}
				}
			}
		}

		// Ramp up batch sizes.
		inoBatchSize *= 2
		const MAX_INO_BATCH = _INO_STEP * 64
		if inoBatchSize >= MAX_INO_BATCH {
			inoBatchSize = MAX_INO_BATCH
		}
	}
}

func (fs *Fs) mountHeartBeat() error {
	_, err := fs.Transact(func(tx fdb.Transaction) (interface{}, error) {
		heartBeatKey := tuple.Tuple{"hafs", fs.fsName, "client", fs.clientId, "heartbeat"}
		lastSeenBytes := [8]byte{}
		binary.LittleEndian.PutUint64(lastSeenBytes[:], uint64(time.Now().Unix()))
		tx.Set(heartBeatKey, lastSeenBytes[:])
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
			if errors.Is(err, ErrDetached) {
				// Must be done in new goroutine to prevent deadlock.
				go fs.onEviction(fs)
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (fs *Fs) IsDetached() bool {
	return fs.clientDetached.Load()
}

func (fs *Fs) Close() error {
	fs.cancelWorkers()
	fs.workerWg.Wait()
	err := EvictClient(fs.db, fs.fsName, fs.clientId)
	return err
}

func (fs *Fs) ReadTransact(f func(tx fdb.ReadTransaction) (interface{}, error)) (interface{}, error) {
	attachKey := tuple.Tuple{"hafs", fs.fsName, "client", fs.clientId, "attached"}
	fWrapped := func(tx fdb.ReadTransaction) (interface{}, error) {
		attachCheck := tx.Get(attachKey)
		v, err := f(tx)
		if attachCheck.MustGet() == nil {
			return v, ErrDetached
		}
		return v, err
	}
	return fs.db.ReadTransact(fWrapped)
}

func (fs *Fs) Transact(f func(tx fdb.Transaction) (interface{}, error)) (interface{}, error) {
	attachKey := tuple.Tuple{"hafs", fs.fsName, "client", fs.clientId, "attached"}
	fWrapped := func(tx fdb.Transaction) (interface{}, error) {
		attachCheck := tx.Get(attachKey)
		v, err := f(tx)
		if attachCheck.MustGet() == nil {
			return v, ErrDetached
		}
		return v, err
	}
	return fs.db.Transact(fWrapped)
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
	err := stat.UnmarshalBinary(statBytes)
	if err != nil {
		return stat, err
	}
	stat.Ino = fut.ino
	return stat, nil
}

func (fs *Fs) txGetStat(tx fdb.ReadTransaction, ino uint64) futureStat {
	return futureStat{
		ino:   ino,
		bytes: tx.Get(tuple.Tuple{"hafs", fs.fsName, "ino", ino, "stat"}),
	}
}

func (fs *Fs) txSetStat(tx fdb.Transaction, stat Stat) {
	statBytes, err := stat.MarshalBinary()
	if err != nil {
		panic(err)
	}
	tx.Set(tuple.Tuple{"hafs", fs.fsName, "ino", stat.Ino, "stat"}, statBytes)
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
	err := dirEnt.UnmarshalBinary(dirEntBytes)
	dirEnt.Name = fut.name
	return dirEnt, err
}

func (fs *Fs) txGetDirEnt(tx fdb.ReadTransaction, dirIno uint64, name string) futureGetDirEnt {
	return futureGetDirEnt{
		name:  name,
		bytes: tx.Get(tuple.Tuple{"hafs", fs.fsName, "ino", dirIno, "child", name}),
	}
}

func (fs *Fs) txSetDirEnt(tx fdb.Transaction, dirIno uint64, ent DirEnt) {
	dirEntBytes, err := ent.MarshalBinary()
	if err != nil {
		panic(err)
	}
	tx.Set(tuple.Tuple{"hafs", fs.fsName, "ino", dirIno, "child", ent.Name}, dirEntBytes)
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
	kvs := tx.GetRange(tuple.Tuple{"hafs", fs.fsName, "ino", dirIno, "child"}, fdb.RangeOptions{
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

	if len(name) > NAME_MAX {
		return Stat{}, ErrNameTooLong
	}

	dirStatFut := fs.txGetStat(tx, dirIno)
	getDirEntFut := fs.txGetDirEnt(tx, dirIno, name)

	dirStat, err := dirStatFut.Get()
	if err != nil {
		return Stat{}, err
	}

	if dirStat.Nlink == 0 {
		return Stat{}, ErrNotExist
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
			Subvolume: dirStat.Subvolume,
			Flags:     0,
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

		if dirStat.Flags&FLAG_SUBVOLUME != 0 {
			stat.Subvolume = dirStat.Ino
		}

		if opts.Mode&S_IFMT == S_IFREG {
			// Only files inherit storage from the parent directory.
			if dirStat.Flags&FLAG_EXTERNAL_STORAGE != 0 {
				stat.Flags |= FLAG_EXTERNAL_STORAGE
				stat.Storage = dirStat.Storage
			}
		}

		fs.txSubvolumeInodeDelta(tx, stat.Subvolume, 1)
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

	if dirStat.Mtime().Before(now.Add(-fs.relMtime)) {
		dirStat.SetMtime(now)
		dirStat.SetAtime(now)
		fs.txSetStat(tx, dirStat)
	}

	if stat.Mode&S_IFMT == S_IFLNK {
		tx.Set(tuple.Tuple{"hafs", fs.fsName, "ino", stat.Ino, "target"}, opts.LinkTarget)
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

		if dirStat.Subvolume != stat.Subvolume {
			if dirStat.Flags&FLAG_SUBVOLUME == 0 {
				return Stat{}, ErrInvalid
			}
			if dirStat.Ino != stat.Subvolume {
				return Stat{}, ErrInvalid
			}
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

		if dirStat.Mtime().Before(now.Add(-fs.relMtime)) {
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

func (fs *Fs) txSubvolumeCountDelta(tx fdb.Transaction, subvolume uint64, counter string, delta int64) {
	if delta == 0 {
		return
	}
	const COUNTER_SHARDS = 16
	counterShardIdx := uint64(fastrand.FastRand() % COUNTER_SHARDS)
	deltaBytes := [8]byte{}
	binary.LittleEndian.PutUint64(deltaBytes[:], uint64(delta))
	tx.Add(tuple.Tuple{"hafs", fs.fsName, "ino", subvolume, counter, counterShardIdx}, deltaBytes[:])
}

func (fs *Fs) txSubvolumeByteDelta(tx fdb.Transaction, subvolume uint64, delta int64) {
	fs.txSubvolumeCountDelta(tx, subvolume, "bcnt", delta)
}

func (fs *Fs) txSubvolumeInodeDelta(tx fdb.Transaction, subvolume uint64, delta int64) {
	fs.txSubvolumeCountDelta(tx, subvolume, "icnt", delta)
}

func (fs *Fs) txSubvolumeCount(tx fdb.ReadTransaction, subvolume uint64, counter string) (uint64, error) {
	kvs := tx.GetRange(tuple.Tuple{"hafs", fs.fsName, "ino", subvolume, counter}, fdb.RangeOptions{
		Limit: 512, // Should be large enough to cover all counter shards.
	}).GetSliceOrPanic()
	v := int64(0)
	for _, kv := range kvs {
		if len(kv.Value) != 8 {
			return 0, errors.New("unexpected overflow or invalid counter value")
		}
		v += int64(binary.LittleEndian.Uint64(kv.Value))
	}
	return uint64(v), nil
}

func (fs *Fs) txSubvolumeByteCount(tx fdb.ReadTransaction, subvolume uint64) (uint64, error) {
	return fs.txSubvolumeCount(tx, subvolume, "bcnt")
}

func (fs *Fs) txSubvolumeInodeCount(tx fdb.ReadTransaction, subvolume uint64) (uint64, error) {
	return fs.txSubvolumeCount(tx, subvolume, "icnt")
}

func (fs *Fs) SubvolumeByteCount(subvolume uint64) (uint64, error) {
	v, err := fs.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
		v, err := fs.txSubvolumeByteCount(tx, subvolume)
		return v, err
	})
	if err != nil {
		return 0, err
	}
	return v.(uint64), nil
}

func (fs *Fs) SubvolumeInodeCount(subvolume uint64) (uint64, error) {
	v, err := fs.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
		v, err := fs.txSubvolumeInodeCount(tx, subvolume)
		return v, err
	})
	if err != nil {
		return 0, err
	}
	return v.(uint64), err
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
			fs.txSubvolumeByteDelta(tx, stat.Subvolume, -int64(stat.Size))
			fs.txSubvolumeInodeDelta(tx, stat.Subvolume, -1)
			tx.Set(tuple.Tuple{"hafs", fs.fsName, "unlinked", dirEnt.Ino}, []byte{})
		}
		tx.Clear(tuple.Tuple{"hafs", fs.fsName, "ino", dirIno, "child", name})
		return nil, nil
	})
	return err
}

type HafsFile interface {
	WriteData([]byte, uint64) (uint32, error)
	ReadData([]byte, uint64) (uint32, error)
	Fsync() error
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
	const MAX_WRITE = 32 * CHUNK_SIZE

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
			firstChunkKey := tuple.Tuple{"hafs", f.fs.fsName, "ino", f.ino, "data", firstChunkNo}
			chunk := tx.Get(firstChunkKey).MustGet()
			zeroExpandChunk(&chunk)
			copy(chunk[firstChunkOffset:firstChunkOffset+firstWriteCount], remainingBuf)
			currentOffset += firstWriteCount
			remainingBuf = remainingBuf[firstWriteCount:]
			tx.Set(firstChunkKey, zeroTrimChunk(chunk))
		}

		for {
			key := tuple.Tuple{"hafs", f.fs.fsName, "ino", f.ino, "data", currentOffset / CHUNK_SIZE}
			if len(remainingBuf) >= CHUNK_SIZE {
				tx.Set(key, zeroTrimChunk(remainingBuf[:CHUNK_SIZE]))
				currentOffset += CHUNK_SIZE
				remainingBuf = remainingBuf[CHUNK_SIZE:]
			} else {
				chunk := tx.Get(key).MustGet()
				zeroExpandChunk(&chunk)
				copy(chunk, remainingBuf)
				tx.Set(key, zeroTrimChunk(chunk))
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
			f.fs.txSubvolumeByteDelta(tx, stat.Subvolume, int64(newSize)-int64(stat.Size))
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
func (f *foundationDBFile) ReadData(buf []byte, offset uint64) (uint32, error) {

	const MAX_READ = 32 * CHUNK_SIZE

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
			if overshoot >= uint64(len(remainingBuf)) {
				return 0, io.EOF
			}
			remainingBuf = remainingBuf[:uint64(len(remainingBuf))-overshoot]
		}

		// Deal with the first unaligned and undersized chunk.
		if currentOffset%CHUNK_SIZE != 0 || len(remainingBuf) < CHUNK_SIZE {

			firstChunkNo := currentOffset / CHUNK_SIZE
			firstChunkOffset := currentOffset % CHUNK_SIZE
			firstReadCount := CHUNK_SIZE - firstChunkOffset
			if firstReadCount > uint64(len(remainingBuf)) {
				firstReadCount = uint64(len(remainingBuf))
			}

			firstChunkKey := tuple.Tuple{"hafs", f.fs.fsName, "ino", f.ino, "data", firstChunkNo}
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

		nChunks := uint64(len(remainingBuf)) / CHUNK_SIZE
		if (len(remainingBuf) % CHUNK_SIZE) != 0 {
			nChunks += 1
		}
		chunkFutures := make([]fdb.FutureByteSlice, 0, nChunks)

		// Read all chunks in parallel using futures.
		for i := uint64(0); i < nChunks; i++ {
			key := tuple.Tuple{"hafs", f.fs.fsName, "ino", f.ino, "data", (currentOffset / CHUNK_SIZE) + i}
			chunkFutures = append(chunkFutures, tx.Get(key))
		}

		for i := uint64(0); i < nChunks; i++ {
			chunk := chunkFutures[i].MustGet()
			zeroExpandChunk(&chunk)
			n := copy(remainingBuf, chunk)
			currentOffset += uint64(n)
			remainingBuf = remainingBuf[n:]
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
func (f *foundationDBFile) Close() error { return nil }

type invalidFile struct{}

func (f *invalidFile) WriteData(buf []byte, offset uint64) (uint32, error) { return 0, ErrInvalid }
func (f *invalidFile) ReadData(buf []byte, offset uint64) (uint32, error)  { return 0, ErrInvalid }
func (f *invalidFile) Fsync() error                                        { return ErrInvalid }
func (f *invalidFile) Close() error                                        { return nil }

type objectStoreReadOnlyFile struct {
	storageObject ReaderAtCloser
}

func (f *objectStoreReadOnlyFile) WriteData(buf []byte, offset uint64) (uint32, error) {
	return 0, ErrNotSupported
}

func (f *objectStoreReadOnlyFile) ReadData(buf []byte, offset uint64) (uint32, error) {
	n, err := f.storageObject.ReadAt(buf, int64(offset))
	return uint32(n), err
}

func (f *objectStoreReadOnlyFile) Fsync() error {
	return nil
}

func (f *objectStoreReadOnlyFile) Close() error {
	return f.storageObject.Close()
}

type objectStoreReadWriteFile struct {
	fs            *Fs
	ino           uint64
	dirty         atomicBool
	objectStorage ObjectStorageEngine
	flushLock     sync.Mutex
	tmpFile       *os.File
}

func (f *objectStoreReadWriteFile) WriteData(buf []byte, offset uint64) (uint32, error) {
	f.dirty.Store(true)
	n, err := f.tmpFile.WriteAt(buf, int64(offset))
	return uint32(n), err
}

func (f *objectStoreReadWriteFile) ReadData(buf []byte, offset uint64) (uint32, error) {
	n, err := f.tmpFile.ReadAt(buf, int64(offset))
	return uint32(n), err
}

func (f *objectStoreReadWriteFile) Fsync() error {

	dirty := f.dirty.Load()
	if !dirty {
		return nil
	}

	f.flushLock.Lock()
	defer f.flushLock.Unlock()

	size, err := f.objectStorage.Write(f.fs.fsName, f.ino, f.tmpFile)
	if err != nil {
		return err
	}

	_, err = f.fs.Transact(func(tx fdb.Transaction) (interface{}, error) {
		stat, err := f.fs.txGetStat(tx, f.ino).Get()
		if err != nil {
			return nil, err
		}
		f.fs.txSubvolumeByteDelta(tx, stat.Subvolume, size-int64(stat.Size))
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

func (f *objectStoreReadWriteFile) Flush() error {
	return f.Fsync()
}

func (f *objectStoreReadWriteFile) Close() error {
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

		// Might happen as a result of client side caching.
		if stat.Nlink == 0 {
			// N.B. don't return ErrNotExist, the file might exist but the cache is out of date.
			return nil, ErrInvalid
		}

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
	if stat.Flags&FLAG_EXTERNAL_STORAGE == 0 {
		f = &foundationDBFile{
			fs:  fs,
			ino: stat.Ino,
		}
	} else {
		objectStorage, err := fs.objectStorage.Get(stat.Storage)
		if err != nil {
			return nil, Stat{}, err
		}

		if stat.Size == 0 {
			tmpFile, err := os.CreateTemp("", "")
			if err != nil {
				return nil, Stat{}, err
			}
			// XXX Make file anonymous, it would be nice to create it like this.
			err = os.Remove(tmpFile.Name())
			if err != nil {
				return nil, Stat{}, err
			}

			f = &objectStoreReadWriteFile{
				fs:            fs,
				ino:           stat.Ino,
				dirty:         atomicBool{0},
				objectStorage: objectStorage,
				tmpFile:       tmpFile,
			}
		} else {
			storageObject, err := objectStorage.Open(fs.fsName, stat.Ino)
			if err != nil {
				return nil, Stat{}, err
			}
			f = &objectStoreReadOnlyFile{
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
	stat, err := fs.Mknod(dirIno, name, MknodOpts{
		Truncate: opts.Truncate,
		Mode:     (^S_IFMT & opts.Mode) | S_IFREG,
		Uid:      opts.Uid,
		Gid:      opts.Gid,
	})
	if err != nil {
		return nil, Stat{}, err
	}

	var f HafsFile
	if stat.Flags&FLAG_EXTERNAL_STORAGE == 0 {
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

		objectStorage, err := fs.objectStorage.Get(stat.Storage)
		if err != nil {
			return nil, Stat{}, err
		}

		f = &objectStoreReadWriteFile{
			fs:            fs,
			ino:           stat.Ino,
			objectStorage: objectStorage,
			tmpFile:       tmpFile,
		}
	}
	return f, stat, err
}

func (fs *Fs) ReadSymlink(ino uint64) ([]byte, error) {
	l, err := fs.Transact(func(tx fdb.Transaction) (interface{}, error) {
		statFut := fs.txGetStat(tx, ino)
		lFut := tx.Get(tuple.Tuple{"hafs", fs.fsName, "ino", ino, "target"})
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

		fs.txSubvolumeByteDelta(tx, stat.Subvolume, int64(opts.Size)-int64(stat.Size))
		stat.Size = opts.Size

		if stat.Mode&S_IFMT != S_IFREG {
			return Stat{}, ErrInvalid
		}

		if stat.Size != 0 {
			if stat.Flags&FLAG_EXTERNAL_STORAGE != 0 {
				// We don't support truncating object storage files for now.
				return Stat{}, ErrNotSupported
			}

			// Don't allow arbitrarily setting unrealistically huge file sizes
			// that risk overflows and other strange problems by going past sensible limits.
			if stat.Size > 0xFFFF_FFFF_FFFF {
				return Stat{}, ErrInvalid
			}

			clearBegin := (stat.Size + (CHUNK_SIZE - stat.Size%4096)) / CHUNK_SIZE
			_, clearEnd := tuple.Tuple{"hafs", fs.fsName, "ino", ino, "data"}.FDBRangeKeys()
			tx.ClearRange(fdb.KeyRange{
				Begin: tuple.Tuple{"hafs", fs.fsName, "ino", ino, "data", clearBegin},
				End:   clearEnd,
			})
			lastChunkIdx := stat.Size / CHUNK_SIZE
			lastChunkSize := stat.Size % CHUNK_SIZE
			lastChunkKey := tuple.Tuple{"hafs", fs.fsName, "ino", ino, "data", lastChunkIdx}
			if lastChunkSize == 0 {
				tx.Clear(lastChunkKey)
			}
		} else {
			tx.ClearRange(tuple.Tuple{"hafs", fs.fsName, "ino", ino, "data"})
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
	stat, err := fs.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
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
				tx.Set(tuple.Tuple{"hafs", fs.fsName, "unlinked", toStat.Ino}, []byte{})
			}
		}

		if toDirIno != fromDirIno {

			// Enforce subvolume invariants.
			if toDirStat.Flags&FLAG_SUBVOLUME != 0 && fromDirStat.Flags&FLAG_SUBVOLUME != 0 {
				return nil, ErrInvalid
			} else if toDirStat.Flags&FLAG_SUBVOLUME != 0 {
				if toDirStat.Ino != fromDirStat.Subvolume {
					return nil, ErrInvalid
				}
			} else if fromDirStat.Flags&FLAG_SUBVOLUME != 0 {
				if fromDirStat.Ino != toDirStat.Subvolume {
					return nil, ErrInvalid
				}
			} else { // Neither are subvolumes
				if toDirStat.Subvolume != fromDirStat.Subvolume {
					return nil, ErrInvalid
				}
			}

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

		tx.Clear(tuple.Tuple{"hafs", fs.fsName, "ino", fromDirIno, "child", fromName})
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
	stats     []Stat
	isPlus    bool
	done      bool
}

func (di *DirIter) fill() error {
	const BATCH_SIZE = 512

	_, err := di.fs.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
		// XXX should we confirm the directory still exists?
		kvs := tx.GetRange(di.iterRange, fdb.RangeOptions{
			Limit: BATCH_SIZE,
		}).GetSliceOrPanic()

		if len(kvs) != 0 {
			nextBegin, err := fdb.Strinc(kvs[len(kvs)-1].Key)
			if err != nil {
				return nil, err
			}
			di.iterRange.Begin = fdb.Key(nextBegin)
		} else {
			di.iterRange.Begin = di.iterRange.End
		}

		ents := make([]DirEnt, 0, len(kvs))

		statFuts := []futureStat{}
		if di.isPlus {
			statFuts = make([]futureStat, 0, len(kvs))
		}

		for _, kv := range kvs {
			keyTuple, err := tuple.Unpack(kv.Key)
			if err != nil {
				return nil, err
			}
			name := keyTuple[len(keyTuple)-1].(string)
			dirEnt := DirEnt{}
			err = dirEnt.UnmarshalBinary(kv.Value)
			if err != nil {
				return nil, err
			}
			dirEnt.Name = name
			ents = append(ents, dirEnt)
			if di.isPlus {
				statFuts = append(statFuts, di.fs.txGetStat(tx, dirEnt.Ino))
			}
		}

		// Reverse entries so we can pop them off in the right order.
		for i, j := 0, len(ents)-1; i < j; i, j = i+1, j-1 {
			ents[i], ents[j] = ents[j], ents[i]
		}

		stats := make([]Stat, 0, len(statFuts))
		// Read stats in reverse order
		for i := len(statFuts) - 1; i >= 0; i -= 1 {
			stat, err := statFuts[i].Get()
			if err != nil {
				return nil, err
			}
			stats = append(stats, stat)
		}

		if len(ents) < BATCH_SIZE {
			di.done = true
		}

		di.ents = ents
		di.stats = stats
		return nil, nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (di *DirIter) next(ent *DirEnt, stat *Stat) error {
	di.lock.Lock()
	defer di.lock.Unlock()

	if len(di.ents) == 0 && di.done {
		return io.EOF
	}

	// Fill initial listing, otherwise we should always have something.
	if len(di.ents) == 0 {
		err := di.fill()
		if err != nil {
			return err
		}
		if len(di.ents) == 0 && di.done {
			return io.EOF
		}
	}

	if ent != nil {
		*ent = di.ents[len(di.ents)-1]
	}
	di.ents = di.ents[:len(di.ents)-1]

	if di.isPlus {
		if stat != nil {
			*stat = di.stats[len(di.stats)-1]
		}
		di.stats = di.stats[:len(di.stats)-1]
	}

	return nil
}

func (di *DirIter) Next() (DirEnt, error) {
	ent := DirEnt{}
	err := di.next(&ent, nil)
	return ent, err
}

func (di *DirIter) NextPlus() (DirEnt, Stat, error) {
	ent := DirEnt{}
	stat := Stat{}
	err := di.next(&ent, &stat)
	return ent, stat, err
}

func (di *DirIter) Unget(ent DirEnt) {
	di.lock.Lock()
	defer di.lock.Unlock()
	if di.isPlus {
		panic("api misuse")
	}
	di.ents = append(di.ents, ent)
	di.done = false
}

func (di *DirIter) UngetPlus(ent DirEnt, stat Stat) {
	di.lock.Lock()
	defer di.lock.Unlock()
	if !di.isPlus {
		panic("api misuse")
	}
	di.ents = append(di.ents, ent)
	di.stats = append(di.stats, stat)
	di.done = false
}

func (fs *Fs) iterDirEnts(dirIno uint64, plus bool) (*DirIter, error) {
	iterBegin, iterEnd := tuple.Tuple{"hafs", fs.fsName, "ino", dirIno, "child"}.FDBRangeKeys()
	di := &DirIter{
		fs: fs,
		iterRange: fdb.KeyRange{
			Begin: iterBegin,
			End:   iterEnd,
		},
		ents:   []DirEnt{},
		isPlus: plus,
		done:   false,
	}
	err := di.fill()
	return di, err
}

func (fs *Fs) IterDirEnts(dirIno uint64) (*DirIter, error) {
	return fs.iterDirEnts(dirIno, false)
}

func (fs *Fs) IterDirEntsPlus(dirIno uint64) (*DirIter, error) {
	return fs.iterDirEnts(dirIno, true)
}

func (fs *Fs) GetXAttr(ino uint64, name string) ([]byte, error) {
	x, err := fs.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
		switch name {
		case "hafs.totals":
			statFut := fs.txGetStat(tx, ino)
			bcount, err := fs.txSubvolumeByteCount(tx, ino)
			if err != nil {
				return nil, err
			}
			icount, err := fs.txSubvolumeInodeCount(tx, ino)
			if err != nil {
				return nil, err
			}
			stat, err := statFut.Get()
			if err != nil {
				return nil, err
			}
			if stat.Flags&FLAG_SUBVOLUME == 0 {
				return nil, ErrInvalid
			}
			return []byte(fmt.Sprintf(`{"bytes":%d,"inodes":%d}`, bcount, icount)), nil
		case "hafs.total-bytes", "hafs.total-inodes":
			var (
				count uint64
				err   error
			)
			statFut := fs.txGetStat(tx, ino)
			switch name {
			case "hafs.total-bytes":
				count, err = fs.txSubvolumeByteCount(tx, ino)
				if err != nil {
					return nil, err
				}
			case "hafs.total-inodes":
				count, err = fs.txSubvolumeInodeCount(tx, ino)
				if err != nil {
					return nil, err
				}
			}
			stat, err := statFut.Get()
			if err != nil {
				return nil, err
			}
			if stat.Flags&FLAG_SUBVOLUME == 0 {
				return nil, ErrInvalid
			}
			return []byte(fmt.Sprintf("%d", count)), nil
		default:
			statFut := fs.txGetStat(tx, ino)
			xFut := tx.Get(tuple.Tuple{"hafs", fs.fsName, "ino", ino, "xattr", name})
			_, err := statFut.Get()
			if err != nil {
				return nil, err
			}
			return xFut.MustGet(), nil
		}

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
		tx.Set(tuple.Tuple{"hafs", fs.fsName, "ino", ino, "xattr", name}, data)

		switch name {
		case "hafs.storage":
			if stat.Mode&S_IFMT != S_IFDIR {
				return nil, ErrInvalid
			}
			stat.Flags |= FLAG_EXTERNAL_STORAGE
			stat.Storage = string(data)
			err := fs.objectStorage.Validate(stat.Storage)
			if err != nil {
				return nil, fmt.Errorf("unable to validate storage specification: %s", err)
			}
			fs.txSetStat(tx, stat)
		case "hafs.total-bytes", "hafs.total-inodes", "hafs.totals":
			return nil, ErrInvalid
		case "hafs.subvolume":
			if stat.Mode&S_IFMT != S_IFDIR {
				return nil, ErrInvalid
			}
			if fs.txDirHasChildren(tx, stat.Ino) {
				return nil, ErrInvalid
			}
			flag := FLAG_SUBVOLUME
			switch string(data) {
			case "true":
				stat.Flags |= flag
			default:
				return nil, ErrInvalid
			}
			fs.txSetStat(tx, stat)
		default:
		}
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
			if stat.Mode&S_IFMT != S_IFDIR {
				return nil, ErrInvalid
			}
			stat.Flags &= ^FLAG_EXTERNAL_STORAGE
			stat.Storage = ""
			fs.txSetStat(tx, stat)
		case "hafs.subvolume":
			if stat.Mode&S_IFMT != S_IFDIR {
				return nil, ErrInvalid
			}
			if fs.txDirHasChildren(tx, stat.Ino) {
				return nil, ErrInvalid
			}
			flag := FLAG_SUBVOLUME
			stat.Flags &= ^flag
			fs.txSetStat(tx, stat)
		default:
		}
		tx.Clear(tuple.Tuple{"hafs", fs.fsName, "ino", ino, "xattr", name})
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
		kvs := tx.GetRange(tuple.Tuple{"hafs", fs.fsName, "ino", ino, "xattr"}, fdb.RangeOptions{}).GetSliceOrPanic()
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
	Owner    uint64
	ClientId string
}

func (lr *exclusiveLockRecord) MarshalBinary() ([]byte, error) {
	bufsz := 2*binary.MaxVarintLen64 + len(lr.ClientId)
	buf := make([]byte, bufsz, bufsz)
	b := buf
	b = b[binary.PutUvarint(b, lr.Owner):]
	b = b[binary.PutUvarint(b, uint64(len(lr.ClientId))):]
	b = b[copy(b, lr.ClientId):]
	return buf[:len(buf)-len(b)], nil
}

func (lr *exclusiveLockRecord) UnmarshalBinary(buf []byte) error {
	r := bytes.NewReader(buf)
	lr.Owner, _ = binary.ReadUvarint(r)
	v, _ := binary.ReadUvarint(r)
	var sb strings.Builder
	sb.Grow(int(v))
	io.CopyN(&sb, r, int64(v))
	lr.ClientId = sb.String()
	return nil
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

		exclusiveLockKey := tuple.Tuple{"hafs", fs.fsName, "ino", ino, "lock", "exclusive"}

		switch opts.Typ {
		case LOCK_NONE:
			exclusiveLockBytes := tx.Get(exclusiveLockKey).MustGet()
			if exclusiveLockBytes != nil {
				exclusiveLock := exclusiveLockRecord{}
				err := exclusiveLock.UnmarshalBinary(exclusiveLockBytes)
				if err != nil {
					return false, err
				}
				// The lock isn't owned by this client.
				if exclusiveLock.ClientId != fs.clientId {
					return false, nil
				}
				// The request isn't for this owner.
				if exclusiveLock.Owner != opts.Owner {
					return false, nil
				}
				tx.Clear(exclusiveLockKey)
			} else {
				sharedLockKey := tuple.Tuple{"hafs", fs.fsName, "ino", ino, "lock", "shared", fs.clientId, opts.Owner}
				tx.Clear(sharedLockKey)
			}
			tx.Clear(tuple.Tuple{"hafs", fs.fsName, "client", fs.clientId, "lock", ino, opts.Owner})
			return true, nil
		case LOCK_SHARED:
			exclusiveLockBytes := tx.Get(exclusiveLockKey).MustGet()
			if exclusiveLockBytes != nil {
				return false, nil
			}
			tx.Set(tuple.Tuple{"hafs", fs.fsName, "ino", ino, "lock", "shared", fs.clientId, opts.Owner}, []byte{})
			tx.Set(tuple.Tuple{"hafs", fs.fsName, "client", fs.clientId, "lock", ino, opts.Owner}, []byte{})
			return true, nil
		case LOCK_EXCLUSIVE:
			exclusiveLockBytes := tx.Get(exclusiveLockKey).MustGet()
			if exclusiveLockBytes != nil {
				return false, nil
			}
			sharedLocks := tx.GetRange(tuple.Tuple{"hafs", fs.fsName, "ino", ino, "lock", "shared"}, fdb.RangeOptions{
				Limit: 1,
			}).GetSliceOrPanic()
			if len(sharedLocks) > 0 {
				return false, nil
			}
			exclusiveLock := exclusiveLockRecord{
				ClientId: fs.clientId,
				Owner:    opts.Owner,
			}
			exclusiveLockBytes, err := exclusiveLock.MarshalBinary()
			if err != nil {
				return false, err
			}
			tx.Set(exclusiveLockKey, exclusiveLockBytes)
			tx.Set(tuple.Tuple{"hafs", fs.fsName, "client", fs.clientId, "lock", ino, opts.Owner}, []byte{})
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
			exclusiveLockKey := tuple.Tuple{"hafs", fs.fsName, "ino", ino, "lock", "exclusive"}
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
		exclusiveLockKey := tuple.Tuple{"hafs", fs.fsName, "ino", ino, "lock", "exclusive"}
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

type RemoveExpiredUnlinkedOptions struct {
	RemovalDelay time.Duration
	OnRemoval    func(*Stat)
}

func (fs *Fs) RemoveExpiredUnlinked(opts RemoveExpiredUnlinkedOptions) (uint64, error) {

	iterBegin, iterEnd := tuple.Tuple{"hafs", fs.fsName, "unlinked"}.FDBRangeKeys()

	iterRange := fdb.KeyRange{
		Begin: iterBegin,
		End:   iterEnd,
	}

	nRemoved := &atomicUint64{}

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
			return nRemoved.Load(), err
		}

		kvs := v.([]fdb.KeyValue)

		if len(kvs) == 0 {
			break
		}

		nextBegin, err := fdb.Strinc(kvs[len(kvs)-1].Key)
		if err != nil {
			return nRemoved.Load(), err
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
					ino := tupleElem2u64(keyTuple[len(keyTuple)-1])
					futureStats = append(futureStats, fs.txGetStat(tx, ino))
				}

				expiredStats := make([]Stat, 0, len(futureStats))

				now := time.Now()
				for _, futureStat := range futureStats {
					stat, err := futureStat.Get()
					if errors.Is(err, ErrNotExist) {
						continue
					}
					if err != nil {
						return nil, err
					}

					if now.After(stat.Ctime().Add(opts.RemovalDelay)) {
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

			for i := range expiredStats {
				stat := &expiredStats[i]
				if stat.Mode&S_IFMT != S_IFREG {
					continue
				}

				if stat.Storage != "" {
					objectStorage, err := fs.objectStorage.Get(stat.Storage)
					if err != nil {
						return err
					}
					err = objectStorage.Remove(fs.fsName, stat.Ino)
					if err != nil {
						return err
					}
				}
			}

			_, err = fs.Transact(func(tx fdb.Transaction) (interface{}, error) {
				for i := range expiredStats {
					stat := &expiredStats[i]
					tx.Clear(tuple.Tuple{"hafs", fs.fsName, "unlinked", stat.Ino})
					tx.ClearRange(tuple.Tuple{"hafs", fs.fsName, "ino", stat.Ino})
				}
				return nil, nil
			})
			if err != nil {
				return err
			}

			if opts.OnRemoval != nil {
				for i := range expiredStats {
					opts.OnRemoval(&expiredStats[i])
				}
			}

			nRemoved.Add(uint64(len(expiredStats)))
			return nil

		})

	}

	err := errg.Wait()
	if err != nil {
		return nRemoved.Load(), err
	}

	return nRemoved.Load(), nil
}

type FsStats struct {
	UsedBytes uint64
	FreeBytes uint64
}

func (fs *Fs) FsStats() (FsStats, error) {

	fsStats := FsStats{}

	v, err := fs.db.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
		return tx.Get(fdb.Key("\xFF\xFF/status/json")).MustGet(), nil
	})
	if err != nil {
		return FsStats{}, err
	}

	var p fastjson.Parser

	status, err := p.ParseBytes(v.([]byte))
	if err != nil {
		return fsStats, err
	}

	processes := status.GetObject("cluster", "processes")
	if processes != nil {
		processes.Visit(func(key []byte, v *fastjson.Value) {
			fsStats.FreeBytes += v.GetUint64("disk", "free_bytes")
			fsStats.UsedBytes += v.GetUint64("disk", "total_bytes")
		})
	}

	return fsStats, nil
}
