package hafs

import (
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/andrewchambers/hafs/testutil"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func tmpDB(t *testing.T) fdb.Database {
	db := testutil.NewFDBTestServer(t).Dial()
	err := Mkfs(db, "testfs", MkfsOpts{Overwrite: false})
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestClientTimedOut(t *testing.T) {

	db := testutil.NewFDBTestServer(t).Dial()

	err := Mkfs(db, "testfs", MkfsOpts{})
	if err != nil {
		t.Fatal(err)
	}

	fs, err := Attach(db, "testfs", AttachOpts{})
	if err != nil {
		t.Fatal(err)
	}

	expired, err := IsClientTimedOut(db, "testfs", fs.clientId, time.Duration(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if expired {
		t.Fatal("expected not expired")
	}

	time.Sleep(1 * time.Second)

	expired, err = IsClientTimedOut(db, "testfs", fs.clientId, time.Duration(0))
	if err != nil {
		t.Fatal(err)
	}
	if !expired {
		t.Fatal("expected expired")
	}
}

func TestClientInfo(t *testing.T) {

	db := testutil.NewFDBTestServer(t).Dial()

	err := Mkfs(db, "testfs", MkfsOpts{Overwrite: false})
	if err != nil {
		t.Fatal(err)
	}

	fs, err := Attach(db, "testfs", AttachOpts{})
	if err != nil {
		t.Fatal(err)
	}

	info, ok, err := GetClientInfo(db, "testfs", fs.clientId)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("client missing")
	}

	if info.Pid != int64(os.Getpid()) {
		t.Fatalf("%v", info)
	}

	clients, err := ListClients(db, "testfs")
	if err != nil {
		t.Fatal(err)
	}
	if len(clients) != 1 {
		t.Fatal("unexpected number of clients")
	}

	if clients[0] != info {
		t.Fatal("client info differs from expected")
	}
}

func TestListFilesystems(t *testing.T) {

	db := testutil.NewFDBTestServer(t).Dial()

	for _, name := range []string{"myfs", "zzz"} {
		err := Mkfs(db, name, MkfsOpts{})
		if err != nil {
			t.Fatal(err)
		}
	}

	filesystems, err := ListFilesystems(db)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(filesystems, []string{"myfs", "zzz"}) {
		t.Fatalf("unexpected filesystem list")
	}

}

func TestEvictClient(t *testing.T) {
	db := testutil.NewFDBTestServer(t).Dial()

	err := Mkfs(db, "testfs", MkfsOpts{})
	if err != nil {
		t.Fatal(err)
	}

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

	err = EvictClient(db, "testfs", fs1.clientId)
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

	_, err = fs1.Mknod(ROOT_INO, "f2", MknodOpts{
		Mode: S_IFREG | 0o777,
		Uid:  0,
		Gid:  0,
	})
	if err != ErrDetached {
		t.Fatal(err)
	}

}
