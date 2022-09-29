package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/andrewchambers/hafs"
	"github.com/andrewchambers/hafs/cli"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func usage() {
	fmt.Printf("hafs-mount [OPTS] MOUNTPOINT\n")
	os.Exit(1)
}

func main() {

	flag.Parse()

	if len(flag.Args()) != 1 {
		usage()
	}

	mntDir := flag.Args()[0]

	server, err := fuse.NewServer(
		hafs.NewFuseFs(cli.MustAttach()),
		mntDir,
		&fuse.MountOptions{
			Name:    "hafs",
			Options: []string{
				// XXX why are these not working?
				// "direct_io",
				// "hard_remove",
			},
			AllowOther:           false, // XXX option?
			DisableXAttrs:        false, // TODO implement
			EnableLocks:          true,
			IgnoreSecurityLabels: true, // option?
			Debug:                true,
		})
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to create fuse server: %s\n", err)
		os.Exit(1)
	}

	go server.Serve()

	err = server.WaitMount()
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable wait for mount: %s\n", err)
		os.Exit(1)
	}

	// Serve the file system, until unmounted by calling fusermount -u
	server.Wait()
}
