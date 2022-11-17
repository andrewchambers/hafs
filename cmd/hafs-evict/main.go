package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/andrewchambers/hafs"
	"github.com/andrewchambers/hafs/cli"
)

func main() {
	cli.RegisterClusterFileFlag()
	cli.RegisterFsNameFlag()
	flag.Parse()
	db := cli.MustOpenDatabase()

	args := flag.Args()
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "expecting a single client to evict\n")
		os.Exit(1)
	}

	err := hafs.EvictClient(db, cli.FsName, args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "error evicting client: %s\n", err)
		os.Exit(1)
	}

}
