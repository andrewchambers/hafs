package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/andrewchambers/hafs"
	"github.com/andrewchambers/hafs/cli"
)

func main() {
	unset := flag.Bool("unset", false, "Unset the object storage config, disabling object storage.")
	set := flag.String("set", "", "The storage specfication specification.")
	force := flag.Bool("force", false, "Force the update even when there are active clients using the old specification.")
	cli.RegisterClusterFileFlag()
	cli.RegisterFsNameFlag()
	flag.Parse()

	db := cli.MustOpenDatabase()

	if *set == "" && !*unset {
		objectStorageSpec, err := hafs.GetObjectStorageSpec(db, cli.FsName)
		if err != nil {
			fmt.Fprintf(os.Stderr, "unable to get object storage specification: %s\n", err)
			os.Exit(1)
		}
		_, _ = fmt.Printf("%s\n", objectStorageSpec)
		os.Exit(0)
	}

	if *unset {
		if *set != "" {
			fmt.Fprintf(os.Stderr, "unable to use -unset and -set at the same time.\n")
			os.Exit(1)
		}
	}

	err := hafs.SetObjectStorageSpec(db, cli.FsName, *set, hafs.SetObjectStorageSpecOpts{
		Force: *force,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to set object storage specfication: %s\n", err)
		os.Exit(1)
	}
}
