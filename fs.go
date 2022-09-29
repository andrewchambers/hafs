package fs

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	gofs "io/fs"
	"os"
	"sync"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"golang.org/x/sys/unix"
)

var (
	ErrNotExist  = gofs.ErrNotExist
	ErrExist     = gofs.ErrExist
	ErrNotEmpty  = errors.New("directory is not empty")
	ErrNotDir    = errors.New("not a directory")
	ErrUnmounted = errors.New("filesystem unmounted")
)

const (
	CURRENT_FDB_API_VERSION = 600
	CURRENT_SCHEMA_VERSION  = 1
	ROOT_INO                = 1
	PAGE_SIZE               = 4096
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
	Nchild    uint64
	Uid       uint32
	Gid       uint32
	Rdev      uint32
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
	db            fdb.Database
	mountId       string
	workerWg      *sync.WaitGroup
	cancelWorkers func()
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
			Nchild:    0,
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
		tx.Set(tuple.Tuple{"fs", "nextino"}, []byte{'2'})
		tx.Set(tuple.Tuple{"fs", "ino", ROOT_INO, "stat"}, rootStatBytes)
		return nil, nil
	})
	return err
}

func Mount(db fdb.Database) (*Fs, error) {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "unknown"
	}

	cookie := [16]byte{}
	_, err := rand.Read(cookie[:])
	if err != nil {
		return nil, err
	}

	mountId := fmt.Sprintf("%s.%d.%s", hostname, time.Now().Unix(), hex.EncodeToString(cookie[:]))

	_, err = db.Transact(func(tx fdb.Transaction) (interface{}, error) {
		version := tx.Get(tuple.Tuple{"fs", "version"}).MustGet()
		if version == nil {
			return nil, errors.New("filesystem is not formatted")
		}
		if !bytes.Equal(version, []byte{CURRENT_SCHEMA_VERSION}) {
			return nil, fmt.Errorf("filesystem has different version - expected %d but got %d", CURRENT_SCHEMA_VERSION, version[0])
		}
		return nil, nil
	})
	if err != nil {
		return nil, fmt.Errorf("unable to add mount: %w", err)
	}

	workerCtx, cancelWorkers := context.WithCancel(context.Background())

	fs := &Fs{
		db:            db,
		mountId:       mountId,
		cancelWorkers: cancelWorkers,
		workerWg:      &sync.WaitGroup{},
	}

	err = fs.mountHeartBeat()
	if err != nil {
		_ = fs.Close()
		return nil, err
	}

	fs.workerWg.Add(1)
	go func() {
		defer fs.workerWg.Done()
		fs.mountHeartBeatForever(workerCtx)
	}()

	return fs, nil
}

func (fs *Fs) mountHeartBeat() error {
	lastSeen, err := json.Marshal(time.Now().Unix())
	if err != nil {
		return err
	}
	_, err = fs.db.Transact(func(tx fdb.Transaction) (interface{}, error) {
		tx.Set(tuple.Tuple{"fs", "mounts", fs.mountId, "heartbeat"}, lastSeen)
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
			_ = fs.mountHeartBeat()
		case <-ctx.Done():
			return
		}
	}
}

func (fs *Fs) Close() error {
	fs.cancelWorkers()
	fs.workerWg.Wait()

	_, err := fs.db.Transact(func(tx fdb.Transaction) (interface{}, error) {
		tx.ClearRange(tuple.Tuple{"fs", "mounts", fs.mountId})
		return nil, nil
	})
	if err != nil {
		return fmt.Errorf("unable to remove mount: %w", err)
	}
	return nil
}

func (fs *Fs) ReadTransact(f func(tx fdb.ReadTransaction) (interface{}, error)) (interface{}, error) {
	return fs.db.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
		mountCheck := tx.Get(tuple.Tuple{"fs", "mounts", fs.mountId, "heartbeat"})
		v, err := f(tx)
		if mountCheck.MustGet() == nil {
			return v, ErrUnmounted
		}
		return v, err
	})
}

func (fs *Fs) Transact(f func(tx fdb.Transaction) (interface{}, error)) (interface{}, error) {
	return fs.db.Transact(func(tx fdb.Transaction) (interface{}, error) {
		mountCheck := tx.Get(tuple.Tuple{"fs", "mounts", fs.mountId, "heartbeat"})
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

func (fs *Fs) txNextIno(tx fdb.Transaction) uint64 {
	// XXX If we avoid json for this we can use fdb native increment.
	// XXX Lots of contention, we could use an array of counters and choose one.
	var ino uint64
	nextInoBytes := tx.Get(tuple.Tuple{"fs", "nextino"}).MustGet()
	err := json.Unmarshal(nextInoBytes, &ino)
	if err != nil {
		panic(err)
	}
	nextInoBytes, err = json.Marshal(ino + 1)
	if err != nil {
		panic(err)
	}
	tx.Set(tuple.Tuple{"fs", "nextino"}, nextInoBytes)
	return ino
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
	return dirEnt.(DirEnt), err
}

type MknodOpts struct {
	Truncate bool
	Mode     uint32
	Uid      uint32
	Gid      uint32
	Rdev     uint32
}

func (fs *Fs) Mknod(dirIno uint64, name string, opts MknodOpts) (Stat, error) {

	stat, err := fs.Transact(func(tx fdb.Transaction) (interface{}, error) {
		dirStatFut := fs.txGetStat(tx, dirIno)
		getDirEntFut := fs.txGetDirEnt(tx, dirIno, name)

		dirStat, err := dirStatFut.Get()
		if err != nil {
			return nil, err
		}

		if dirStat.Mode&S_IFMT != S_IFDIR {
			return nil, ErrNotDir
		}

		var stat Stat

		existingDirEnt, err := getDirEntFut.Get()
		if err == nil {
			if !opts.Truncate {
				return nil, ErrExist
			}

			stat, err = fs.txGetStat(tx, existingDirEnt.Ino).Get()
			if err != nil {
				return nil, err
			}

			if stat.Mode&S_IFMT != S_IFREG {
				return nil, errors.New("unable to truncate invalid file type")
			}

			stat.Size = 0
			tx.ClearRange(tuple.Tuple{"fs", "ino", stat.Ino, "page"})
		} else {
			newIno := fs.txNextIno(tx)
			if err != ErrNotExist {
				return 0, err
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
				Nchild:    0,
				Uid:       opts.Uid,
				Gid:       opts.Gid,
				Rdev:      opts.Rdev,
			}
			dirStat.Nchild += 1
			fs.txSetStat(tx, dirStat)
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
			if stat.Nchild != 0 {
				return nil, ErrNotEmpty
			}
		}

		dirStat.Nchild -= 1
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

func (fs *Fs) GetStat(ino uint64) (Stat, error) {
	stat, err := fs.Transact(func(tx fdb.Transaction) (interface{}, error) {
		stat, err := fs.txGetStat(tx, ino).Get()
		return stat, err
	})
	return stat.(Stat), err
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
	return stat.(Stat), err
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
			// We can't move over a directory with children.
			if toStat.Nchild != 0 {
				return nil, ErrNotEmpty
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
			if errors.Is(toDirEntErr, ErrNotExist) {
				toDirStat.Nchild += 1
			}
			toDirStat.SetMtime(now)
			toDirStat.SetCtime(now)
			fs.txSetStat(tx, toDirStat)
			fromDirStat.Nchild -= 1
			fromDirStat.SetMtime(now)
			toDirStat.SetCtime(now)
			fs.txSetStat(tx, fromDirStat)
		} else {
			if toDirEntErr == nil {
				toDirStat.Nchild -= 1
			}
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

/*
func (fs *Fs) Read(ino uint64, buf []byte, off uint64) (uint32, error) {

	const MAX_READ = 128*PAGE_SIZE

	if len(buf) > MAX_READ {
		buf = buf[:MAX_READ]
	}

	for nRead != uint32(len(buf)) {

		pagev, err := fs.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
			var err error
			stat, err = fs.txGetStat().Get()
			if err != nil {
				return nil, err
			}

			if off > stat.Size {
				return nil, io.EOF
			}

			if stat.Size < off + uint64(len(buf)) {
				buf = buf[:stat.Size-off]
			}

			page = tx.Get(tuple.Tuple{"fs", "ino", "page", pageIdx}).MustGet()
			if page == nil {
				return nil, io.EOF
			}
			return nil, nil
		})
		if err != nil {
			return nRead, err
		}

		firstPage := nRead == 0
		pageStart := uint64(0)
		if firstPage {
			pageStart = off%PAGE_SIZE
		}

		len(page)

	}
}
*/

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
			Limit:   BATCH_SIZE,
			Mode:    fdb.StreamingModeIterator, // XXX do we want StreamingModeWantAll ?
			Reverse: false,
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

func (fs *Fs) RemoveExpiredUnlinked(removalDelay time.Duration) (uint64, error) {

	iterBegin, iterEnd := tuple.Tuple{"fs", "unlinked"}.FDBRangeKeys()

	iterRange := fdb.KeyRange{
		Begin: iterBegin,
		End:   iterEnd,
	}

	nRemoved := uint64(0)
	done := false

	for !done {

		nRemovedThisBatch := uint64(0)
		nextIterBegin := fdb.Key([]byte{})

		_, err := fs.Transact(func(tx fdb.Transaction) (interface{}, error) {

			// Reset for retries.
			nRemovedThisBatch = 0
			done = false

			kvs := tx.GetRange(iterRange, fdb.RangeOptions{
				Limit:   128,
				Mode:    fdb.StreamingModeIterator, // XXX do we want StreamingModeWantAll ?
				Reverse: false,
			}).GetSliceOrPanic()

			if len(kvs) != 0 {
				next, err := fdb.Strinc(kvs[len(kvs)-1].Key)
				if err != nil {
					return nil, err
				}
				nextIterBegin = fdb.Key(next)
			} else {
				done = true
			}

			futureStats := make([]futureStat, 0, len(kvs))
			for _, kv := range kvs {
				keyTuple, err := tuple.Unpack(kv.Key)
				if err != nil {
					return nil, err
				}
				ino := uint64(keyTuple[len(keyTuple)-1].(int64))
				futureStats = append(futureStats, fs.txGetStat(tx, ino))
			}

			now := time.Now()
			for _, futureStat := range futureStats {
				stat, err := futureStat.Get()
				if err != nil {
					return nil, err
				}
				if now.After(stat.Ctime().Add(removalDelay)) {
					tx.Clear(tuple.Tuple{"fs", "unlinked", stat.Ino})
					tx.ClearRange(tuple.Tuple{"fs", "ino", stat.Ino})
					nRemovedThisBatch += 1
				}
			}

			return nil, nil

		})
		if err != nil {
			return nRemoved, err
		}

		iterRange.Begin = nextIterBegin
		nRemoved += nRemovedThisBatch
	}

	return nRemoved, nil
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

	// TODO client eviction in parallel with RemoveExpired

	return stats, nil
}
