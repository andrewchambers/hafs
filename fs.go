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
)

var (
	ErrNotExist  = gofs.ErrNotExist
	ErrNotEmpty  = errors.New("directory is not empty")
	ErrUnmounted = errors.New("filesystem unmounted")
)

var (
	ZeroI64Bytes     = []byte{0, 0, 0, 0, 0, 0, 0, 0}
	OneI64Bytes      = []byte{1, 0, 0, 0, 0, 0, 0, 0}
	MinusOneI64Bytes = []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
)

const (
	CURRENT_FDB_API_VERSION = 600
	CURRENT_SCHEMA_VERSION  = 1
	ROOT_INODE              = 1
)

type Inode uint64

type LookupResult struct {
	Ino  Inode
	Mode uint64
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
		tx.Set(tuple.Tuple{"fs", "version"}, []byte{CURRENT_SCHEMA_VERSION})
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
		mountKey := tuple.Tuple{"fs", "mounts", fs.mountId}
		mountCheck := tx.Get(mountKey)
		v, err := f(tx)
		if mountCheck.MustGet() == nil {
			return nil, ErrUnmounted
		}
		return v, err
	})
}

func (fs *Fs) Transact(f func(tx fdb.Transaction) (interface{}, error)) (interface{}, error) {
	return fs.db.Transact(func(tx fdb.Transaction) (interface{}, error) {
		mountKey := tuple.Tuple{"fs", "mounts", fs.mountId}
		mountCheck := tx.Get(mountKey)
		v, err := f(tx)
		if mountCheck.MustGet() == nil {
			return nil, ErrUnmounted
		}
		return v, err
	})
}

func (fs *Fs) txLookup(tx fdb.ReadTransaction, ino Inode, name string) (LookupResult, error) {
	lookupBytes := tx.Get(tuple.Tuple{"fs", ino, "child", name}).MustGet()
	if lookupBytes == nil {
		return LookupResult{}, ErrNotExist
	}
	lookup := LookupResult{}
	err := json.Unmarshal(lookupBytes, &lookup)
	if err != nil {
		return LookupResult{}, err
	}
	return lookup, nil
}

func (fs *Fs) Lookup(ino Inode, name string) (LookupResult, error) {
	lookup, err := fs.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
		lookup, err := fs.txLookup(tx, ino, name)
		return lookup, err
	})
	if err != nil {
		return LookupResult{}, err
	}
	return lookup.(LookupResult), nil
}

func (fs *Fs) Unlink(ino Inode, name string) error {
	_, err := fs.Transact(func(tx fdb.Transaction) (interface{}, error) {
		lookup, err := fs.txLookup(tx, ino, name)
		if err != nil {
			return nil, err
		}

		childNLinkKey := tuple.Tuple{"fs", "ino", lookup.Ino, "nlink"}
		childNChildKey := tuple.Tuple{"fs", "ino", lookup.Ino, "nchild"}

		childNLink := tx.Get(childNLinkKey)
		childNChild := tx.Get(childNChildKey)

		childNChildBytes := childNChild.MustGet()
		if childNChildBytes != nil && !bytes.Equal(childNChildBytes, ZeroI64Bytes) {
			return nil, ErrNotEmpty
		}

		tx.Add(tuple.Tuple{"fs", "ino", ino, "nchild"}, MinusOneI64Bytes)
		tx.Clear(tuple.Tuple{"fs", "ino", ino, "child", name})

		childNLinkBytes := childNLink.MustGet()
		if bytes.Equal(childNLinkBytes, OneI64Bytes) {
			tx.ClearRange(tuple.Tuple{"fs", "ino", lookup.Ino})
		} else {
			tx.Add(childNLinkKey, MinusOneI64Bytes)
		}

		return nil, nil
	})
	return err
}
