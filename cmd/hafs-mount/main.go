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

	cli.RegisterDefaultFlags()
	debugFuse := flag.Bool("debug-fuse", false, "Log fuse messages.")
	cacheDentries := flag.Duration("cache-dentries", 0, "Duration to cache dentry lookups, use with great care.")
	cacheAttributes := flag.Duration("cache-attributes", 0, "Duration to cache file attribute lookups, use with great care.")

	flag.Parse()

	if len(flag.Args()) != 1 {
		usage()
	}

	mntDir := flag.Args()[0]

	fs := cli.MustAttach()
	defer fs.Close()

	cli.RegisterDefaultSignalHandlers(fs)

	server, err := fuse.NewServer(
		hafs.NewFuseFs(fs, hafs.HafsFuseOptions{
			CacheDentries:   *cacheDentries,
			CacheAttributes: *cacheAttributes,
		}),
		mntDir,
		&fuse.MountOptions{
			Name:                 "hafs",
			Options:              []string{},
			AllowOther:           false, // XXX option?
			EnableLocks:          true,
			IgnoreSecurityLabels: true, // option?
			Debug:                *debugFuse,
			MaxWrite:             fuse.MAX_KERNEL_WRITE,
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
