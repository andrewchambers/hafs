package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/andrewchambers/hafs"
	"github.com/andrewchambers/hafs/cli"
)

func main() {
	overwrite := flag.Bool("overwrite", false, "Overwrite any existing filesystem.")
	flag.Parse()
	db := cli.MustOpenDatabase()
	err := hafs.Mkfs(db, hafs.MkfsOpts{
		Overwrite: *overwrite,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to create filesystem: %s\n", err)
		os.Exit(1)
	}
}
