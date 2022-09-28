package cli

import (
	"flag"
	"os"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

var ClusterFile string

func init() {
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

func MustOpenDatabase() fdb.Database {
	db, err := fdb.OpenDatabase(ClusterFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to open database: %s\n", err)
		os.Exit(1)
	}
	return db
}