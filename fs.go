package fs

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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
	ErrUnmounted = errors.New("filesystem unmounted")
)

const (
	CURRENT_FDB_API_VERSION = 600
	CURRENT_SCHEMA_VERSION  = 1
	ROOT_INO                = 1
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
	// Mode & S_IFMT
	Mode uint32
	Ino  uint64
}

type Stat struct {
	Ino       uint64 `json:"-"`
	Size      uint64
	Atime     uint64
	Mtime     uint64
	Ctime     uint64
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

type Fs struct {
	db            fdb.Database
	mountId       string
	workerWg      *sync.WaitGroup
	cancelWorkers func()
}

func init() {
	fdb.MustAPIVersion(CURRENT_FDB_API_VERSION)
}

func Mkfs(db fdb.Database) error {
	_, err := db.Transact(func(tx fdb.Transaction) (interface{}, error) {
		now := time.Now()
		unixNow := uint64(now.Unix())
		unixNowNSec := uint32(unixNow*1_000_000_000 - uint64(now.UnixNano()))

		rootStat := Stat{
			Ino:       ROOT_INO,
			Size:      0,
			Atime:     unixNow,
			Mtime:     unixNow,
			Ctime:     unixNow,
			Atimensec: unixNowNSec,
			Mtimensec: unixNowNSec,
			Ctimensec: unixNowNSec,
			Mode:      S_IFDIR | 0o755,
			Nlink:     1,
			Nchild:    0,
			Uid:       0,
			Gid:       0,
			Rdev:      0,
		}

		rootStatBytes, err := json.Marshal(rootStat)
		if err != nil {
			return nil, err
		}

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
		tx.ClearRange(tuple.Tuple{"fs", "mounts", fs.mountId, "heartbeat"})
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

func (fs *Fs) txStat(tx fdb.ReadTransaction, ino uint64) futureStat {
	return futureStat{
		ino:   ino,
		bytes: tx.Get(tuple.Tuple{"fs", "ino", ino, "stat"}),
	}
}

func (fs *Fs) txSetStat(tx fdb.Transaction, stat Stat) error {
	statBytes, err := json.Marshal(stat)
	if err != nil {
		return err
	}
	tx.Set(tuple.Tuple{"fs", "ino", stat.Ino, "stat"}, statBytes)
	return nil
}

func (fs *Fs) txNextIno(tx fdb.Transaction) (uint64, error) {
	// XXX If we avoid json for this we can use fdb native increment.
	// XXX Lots of contention, we could use an array of counters and choose one.
	var ino uint64
	nextInoBytes := tx.Get(tuple.Tuple{"fs", "nextino"}).MustGet()
	err := json.Unmarshal(nextInoBytes, &ino)
	if err != nil {
		return 0, err
	}
	nextInoBytes, err = json.Marshal(ino + 1)
	if err != nil {
		return 0, err
	}
	tx.Set(tuple.Tuple{"fs", "nextino"}, nextInoBytes)
	return ino, nil
}

type futureGetDirEnt struct {
	bytes fdb.FutureByteSlice
}

func (fut futureGetDirEnt) Get() (DirEnt, error) {
	dirEntBytes := fut.bytes.MustGet()
	if dirEntBytes == nil {
		return DirEnt{}, ErrNotExist
	}
	dirEnt := DirEnt{}
	err := json.Unmarshal(dirEntBytes, &dirEnt)
	return dirEnt, err
}

func (fs *Fs) txGetDirEnt(tx fdb.ReadTransaction, dirIno uint64, name string) futureGetDirEnt {
	return futureGetDirEnt{
		bytes: tx.Get(tuple.Tuple{"fs", "ino", dirIno, "child", name}),
	}
}

func (fs *Fs) txSetDirEnt(tx fdb.Transaction, dirIno uint64, name string, ent DirEnt) error {
	dirEntBytes, err := json.Marshal(ent)
	if err != nil {
		return err
	}
	tx.Set(tuple.Tuple{"fs", "ino", dirIno, "child", name}, dirEntBytes)
	return nil
}

func (fs *Fs) GetDirEnt(dirIno uint64, name string) (DirEnt, error) {
	dirEnt, err := fs.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
		dirEnt, err := fs.txGetDirEnt(tx, dirIno, name).Get()
		return dirEnt, err
	})
	return dirEnt.(DirEnt), err
}

type CreateOpts struct {
	Mode uint32
	Uid  uint32
	Gid  uint32
	Rdev uint32
}

func (fs *Fs) Create(dirIno uint64, name string, opts CreateOpts) (uint64, error) {
	newIno, err := fs.Transact(func(tx fdb.Transaction) (interface{}, error) {
		dirStatFut := fs.txStat(tx, dirIno)
		getDirEntFut := fs.txGetDirEnt(tx, dirIno, name)

		newIno, err := fs.txNextIno(tx)
		if err != nil {
			return 0, err
		}

		dirStat, err := dirStatFut.Get()
		if err != nil {
			return 0, err
		}

		_, err = getDirEntFut.Get()
		if err == nil {
			return 0, ErrExist
		} else {
			if err != ErrNotExist {
				return 0, err
			}
		}

		now := time.Now()
		unixNow := uint64(now.Unix())
		unixNowNSec := uint32(unixNow*1_000_000_000 - uint64(now.UnixNano()))

		newStat := Stat{
			Ino:       newIno,
			Size:      0,
			Atime:     unixNow,
			Mtime:     unixNow,
			Ctime:     unixNow,
			Atimensec: unixNowNSec,
			Mtimensec: unixNowNSec,
			Ctimensec: unixNowNSec,
			Mode:      opts.Mode,
			Nlink:     1,
			Nchild:    0,
			Uid:       opts.Uid,
			Gid:       opts.Gid,
			Rdev:      opts.Rdev,
		}

		err = fs.txSetStat(tx, newStat)
		if err != nil {
			return 0, err
		}

		dirStat.Nchild += 1
		err = fs.txSetStat(tx, dirStat)
		if err != nil {
			return 0, err
		}

		err = fs.txSetDirEnt(tx, dirIno, name, DirEnt{
			Mode: opts.Mode & S_IFMT,
			Ino:  newIno,
		})
		if err != nil {
			return 0, err
		}

		return newIno, nil
	})
	if err != nil {
		return 0, err
	}
	return newIno.(uint64), nil
}

func (fs *Fs) Unlink(dirIno uint64, name string) error {
	_, err := fs.Transact(func(tx fdb.Transaction) (interface{}, error) {

		dirStatFut := fs.txStat(tx, dirIno)

		dirEnt, err := fs.txGetDirEnt(tx, dirIno, name).Get()
		if err != nil {
			return nil, err
		}
		stat, err := fs.txStat(tx, dirEnt.Ino).Get()
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
		err = fs.txSetStat(tx, dirStat)
		if err != nil {
			return nil, err
		}

		if stat.Nlink == 1 {
			tx.ClearRange(tuple.Tuple{"fs", "ino", dirEnt.Ino})
		} else {
			stat.Nlink -= 1
			err = fs.txSetStat(tx, stat)
			if err != nil {
				return nil, err
			}
		}

		tx.Clear(tuple.Tuple{"fs", "ino", dirIno, "child", name})

		return nil, nil
	})
	return err
}

func (fs *Fs) Stat(ino uint64) (Stat, error) {
	stat, err := fs.Transact(func(tx fdb.Transaction) (interface{}, error) {
		stat, err := fs.txStat(tx, ino).Get()
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
		stat, err := fs.txStat(tx, dirEnt.Ino).Get()
		return stat, err
	})
	return stat.(Stat), err
}
