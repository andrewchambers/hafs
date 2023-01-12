package cli

import (
	"flag"
	"fmt"
	"os"
	"os/signal"

	"github.com/andrewchambers/hafs"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"golang.org/x/sys/unix"
)

var FsName string
var ClientDescription string
var ClusterFile string

func RegisterClusterFileFlag() {
	defaultClusterFile := os.Getenv("FDB_CLUSTER_FILE")
	if defaultClusterFile == "" {
		defaultClusterFile = "./fdb.cluster"
		_, err := os.Stat("./fdb.cluster")
		if err != nil {
			defaultClusterFile = "/etc/foundationdb/fdb.cluster"
		}
	}

	flag.StringVar(
		&ClusterFile,
		"cluster-file",
		defaultClusterFile,
		"FoundationDB cluster file, defaults to FDB_CLUSTER_FILE if set, ./fdb.cluster if present, otherwise /etc/foundationdb/fdb.cluster",
	)
}

func RegisterClientDescriptionFlag() {
	flag.StringVar(
		&ClientDescription,
		"client-description",
		"",
		"Optional decription of this fs client.",
	)
}

func RegisterFsNameFlag() {
	flag.StringVar(
		&FsName,
		"fs-name",
		"",
		"Name of the filesystem to interact with.",
	)
}

func RegisterFsSignalHandlers(fs *hafs.Fs) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, unix.SIGINT, unix.SIGTERM)

	go func() {
		<-sigChan
		signal.Reset()
		fmt.Fprintf(os.Stderr, "closing down due to signal...\n")
		err := fs.Close()
		if err != nil {
			fmt.Fprintf(os.Stderr, "error disconnecting client: %s\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}()
}

func MustOpenDatabase() fdb.Database {
	db, err := fdb.OpenDatabase(ClusterFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to open database: %s\n", err)
		os.Exit(1)
	}
	return db
}

func MustAttach(db fdb.Database) *hafs.Fs {
	fs, err := hafs.Attach(db, FsName, hafs.AttachOpts{
		ClientDescription: ClientDescription,
		OnEviction: func(fs *hafs.Fs) {
			fmt.Fprintf(os.Stderr, "client evicted, aborting...\n")
			os.Exit(1)
		},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to connect to filesystem: %s\n", err)
		os.Exit(1)
	}
	return fs
}
