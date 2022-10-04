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
	flag.Parse()
	db := cli.MustOpenDatabase()
	filesystems, err := hafs.ListFilesystems(db)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to create filesystem: %s\n", err)
		os.Exit(1)
	}
	for _, fs := range filesystems {
		_, _ = fmt.Println(fs)
	}
}
