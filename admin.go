package hafs

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

const (
	CURRENT_FDB_API_VERSION = 710
)

func init() {
	fdb.MustAPIVersion(CURRENT_FDB_API_VERSION)
}

type MkfsOpts struct {
	Overwrite bool
}

func Mkfs(db fdb.Database, fsName string, opts MkfsOpts) error {

	if len(fsName) > 255 {
		return errors.New("filesystem name must be less than 256 bytes")
	}

	validNameRune := func(r rune) bool {
		if r == '-' || r == '_' {
			return true
		} else if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') {
			return true
		} else if r >= '0' && r <= '9' {
			return true
		} else {
			return false
		}
	}

	for _, r := range fsName {
		if !validNameRune(r) {
			return errors.New("filesystem names must only contain 'a-z', 'A-Z', '0-9', '-' and _'")
		}
	}

	_, err := db.Transact(func(tx fdb.Transaction) (interface{}, error) {

		if tx.Get(tuple.Tuple{"hafs", fsName, "version"}).MustGet() != nil {
			if !opts.Overwrite {
				return nil, errors.New("filesystem already present")
			}
		}

		now := time.Now()

		rootStat := Stat{
			Ino:       ROOT_INO,
			Subvolume: 0,
			Flags:     FLAG_SUBVOLUME, // The root inode is a subvolume of the filesystem.
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

		rootStatBytes, err := rootStat.MarshalBinary()
		if err != nil {
			return nil, err
		}

		tx.ClearRange(tuple.Tuple{"hafs", fsName})
		tx.Set(tuple.Tuple{"hafs", fsName, "version"}, []byte{CURRENT_SCHEMA_VERSION})
		tx.Set(tuple.Tuple{"hafs", fsName, "ino", ROOT_INO, "stat"}, rootStatBytes)
		tx.Set(tuple.Tuple{"hafs", fsName, "inocntr"}, []byte{0, 0, 0, 0, 0, 0, 0, 1})
		return nil, nil
	})
	return err
}

type RmfsOpts struct {
	Force bool
}

func Rmfs(db fdb.Database, fsName string, opts RmfsOpts) (bool, error) {
	v, err := db.Transact(func(tx fdb.Transaction) (interface{}, error) {
		kvs := tx.GetRange(tuple.Tuple{"hafs", fsName}, fdb.RangeOptions{
			Limit: 2,
		}).GetSliceOrPanic()
		if len(kvs) == 0 {
			return false, nil
		}
		if !opts.Force {
			kvs = tx.GetRange(tuple.Tuple{"hafs", fsName, "ino"}, fdb.RangeOptions{
				Limit: 2,
			}).GetSliceOrPanic()
			if len(kvs) != 1 {
				// The root inode is an exception.
				return false, fmt.Errorf("filesystem is not empty")
			}
			kvs = tx.GetRange(tuple.Tuple{"hafs", fsName, "clients"}, fdb.RangeOptions{
				Limit: 1,
			}).GetSliceOrPanic()
			if len(kvs) != 0 {
				return false, fmt.Errorf("filesystem has connected clients")
			}
			kvs = tx.GetRange(tuple.Tuple{"hafs", fsName, "unlinked"}, fdb.RangeOptions{
				Limit: 1,
			}).GetSliceOrPanic()
			if len(kvs) != 0 {
				return false, fmt.Errorf("filesystem has inodes pending garbage collection")
			}
		}

		tx.ClearRange(tuple.Tuple{"hafs", fsName})
		return true, nil
	})
	if err != nil {
		return false, err
	}
	return v.(bool), nil
}

type ClientInfo struct {
	Id             string `json:",omitempty"`
	Description    string `json:",omitempty"`
	Hostname       string `json:",omitempty"`
	Pid            int64  `json:",omitempty"`
	Exe            string `json:",omitempty"`
	AttachTimeUnix uint64 `json:",omitempty"`
	HeartBeatUnix  uint64 `json:",omitempty"`
}

func GetClientInfo(db fdb.Database, fsName, clientId string) (ClientInfo, bool, error) {

	var ok bool
	var info ClientInfo

	_, err := db.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
		info = ClientInfo{}
		ok = false

		infoBytes := tx.Get(tuple.Tuple{"hafs", fsName, "client", clientId, "info"}).MustGet()
		if infoBytes == nil {
			return nil, nil
		}

		err := json.Unmarshal(infoBytes, &info)
		if err != nil {
			return nil, err
		}

		heartBeatBytes := tx.Get(tuple.Tuple{"hafs", fsName, "client", clientId, "heartbeat"}).MustGet()
		if len(heartBeatBytes) != 8 {
			return nil, errors.New("heart beat bytes are missing or corrupt")
		}
		info.HeartBeatUnix = binary.LittleEndian.Uint64(heartBeatBytes)
		info.Id = clientId
		ok = true
		return nil, nil
	})

	return info, ok, err
}

func ListClients(db fdb.Database, fsName string) ([]ClientInfo, error) {

	clients := []ClientInfo{}

	iterBegin, iterEnd := tuple.Tuple{"hafs", fsName, "clients"}.FDBRangeKeys()

	iterRange := fdb.KeyRange{
		Begin: iterBegin,
		End:   iterEnd,
	}

	for {
		v, err := db.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
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

			client, ok, err := GetClientInfo(db, fsName, clientId)
			if err != nil {
				return clients, err
			}
			if !ok {
				continue
			}
			clients = append(clients, client)
		}
	}

	return clients, nil
}

func IsClientTimedOut(db fdb.Database, fsName, clientId string, clientTimeout time.Duration) (bool, error) {
	timedOut, err := db.Transact(func(tx fdb.Transaction) (interface{}, error) {
		heatBeatKey := tuple.Tuple{"hafs", fsName, "client", clientId, "heartbeat"}
		heartBeatBytes := tx.Get(heatBeatKey).MustGet()
		if len(heartBeatBytes) != 8 {
			return true, nil
		}
		lastSeen := time.Unix(int64(binary.LittleEndian.Uint64(heartBeatBytes)), 0)
		timedOut := lastSeen.Add(clientTimeout).Before(time.Now())
		return timedOut, nil
	})
	if err != nil {
		return false, err
	}
	return timedOut.(bool), nil
}

func tupleElem2u64(elem tuple.TupleElement) uint64 {
	switch elem := elem.(type) {
	case uint64:
		return elem
	case int64:
		return uint64(elem)
	default:
		panic(elem)
	}
}

func txBreakLock(tx fdb.Transaction, fsName, clientId string, ino uint64, owner uint64) error {
	exclusiveLockKey := tuple.Tuple{"hafs", fsName, "ino", ino, "lock", "exclusive"}
	exclusiveLockBytes := tx.Get(exclusiveLockKey).MustGet()
	if exclusiveLockBytes != nil {
		exclusiveLock := exclusiveLockRecord{}
		err := exclusiveLock.UnmarshalBinary(exclusiveLockBytes)
		if err != nil {
			return err
		}
		if exclusiveLock.ClientId == clientId && exclusiveLock.Owner == owner {
			tx.Clear(exclusiveLockKey)
		}
	} else {
		sharedLockKey := tuple.Tuple{"hafs", fsName, "ino", ino, "lock", "shared", clientId, owner}
		tx.Clear(sharedLockKey)
	}
	tx.Clear(tuple.Tuple{"hafs", fsName, "client", clientId, "lock", ino, owner})
	return nil
}

func EvictClient(db fdb.Database, fsName, clientId string) error {

	_, err := db.Transact(func(tx fdb.Transaction) (interface{}, error) {
		// Invalidate all the clients in progress transactions.
		tx.ClearRange(tuple.Tuple{"hafs", fsName, "client", clientId, "attached"})
		return nil, nil
	})
	if err != nil {
		return err
	}

	// Remove all file locks held by the client.
	iterBegin, iterEnd := tuple.Tuple{"hafs", fsName, "client", clientId, "lock"}.FDBRangeKeys()

	iterRange := fdb.KeyRange{
		Begin: iterBegin,
		End:   iterEnd,
	}

	for {
		v, err := db.Transact(func(tx fdb.Transaction) (interface{}, error) {
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

		_, err = db.Transact(func(tx fdb.Transaction) (interface{}, error) {
			for _, kv := range kvs {
				tup, err := tuple.Unpack(kv.Key)
				if err != nil {
					return nil, err
				}
				if len(tup) < 2 {
					return nil, errors.New("corrupt lock entry")
				}
				owner := tupleElem2u64(tup[len(tup)-1])
				ino := tupleElem2u64(tup[len(tup)-2])
				err = txBreakLock(tx, fsName, clientId, ino, owner)
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
	_, err = db.Transact(func(tx fdb.Transaction) (interface{}, error) {
		tx.Clear(tuple.Tuple{"hafs", fsName, "clients", clientId})
		tx.ClearRange(tuple.Tuple{"hafs", fsName, "client", clientId})
		return nil, nil
	})
	if err != nil {
		return err
	}

	return nil
}

type EvictExpiredClientsOptions struct {
	ClientExpiry time.Duration
	OnEviction   func(string)
}

func EvictExpiredClients(db fdb.Database, fsName string, opts EvictExpiredClientsOptions) (uint64, error) {

	nEvicted := uint64(0)

	iterBegin, iterEnd := tuple.Tuple{"hafs", fsName, "clients"}.FDBRangeKeys()

	iterRange := fdb.KeyRange{
		Begin: iterBegin,
		End:   iterEnd,
	}

	for {
		v, err := db.Transact(func(tx fdb.Transaction) (interface{}, error) {
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

			shouldEvict, err := IsClientTimedOut(db, fsName, clientId, opts.ClientExpiry)
			if err != nil {
				return nEvicted, err
			}

			if !shouldEvict {
				continue
			}

			err = EvictClient(db, fsName, clientId)
			if err != nil {
				return nEvicted, err
			}
			if opts.OnEviction != nil {
				opts.OnEviction(clientId)
			}

			nEvicted += 1
		}
	}

	return nEvicted, nil
}
