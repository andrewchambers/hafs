package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/andrewchambers/hafs"
	"github.com/andrewchambers/hafs/cli"
)

func main() {
	force := flag.Bool("force", false, "Remove filesystem without regard for cleanup.")
	cli.RegisterClusterFileFlag()
	cli.RegisterFsNameFlag()
	flag.Parse()
	db := cli.MustOpenDatabase()
	ok, err := hafs.Rmfs(db, cli.FsName, hafs.RmfsOpts{
		Force: *force,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to remove filesystem: %s\n", err)
		os.Exit(1)
	}
	if !ok {
		os.Exit(2)
	}
}
