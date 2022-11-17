package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/andrewchambers/hafs"
	"github.com/andrewchambers/hafs/cli"
)

func main() {
	overwrite := flag.Bool("overwrite", false, "Overwrite any existing filesystem (does not free storage objects).")
	cli.RegisterClusterFileFlag()
	cli.RegisterFsNameFlag()
	flag.Parse()
	db := cli.MustOpenDatabase()
	err := hafs.Mkfs(db, cli.FsName, hafs.MkfsOpts{
		Overwrite: *overwrite,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to create filesystem: %s\n", err)
		os.Exit(1)
	}
}
