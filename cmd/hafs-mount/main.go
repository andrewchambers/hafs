package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

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
	gcUnlinkedInterval := flag.Duration("gc-unlinked-interval", 8*time.Hour, "Unlinked inode garbage collection interval (0 to disable).")
	unlinkRemovalDelay := flag.Duration("unlink-removal-delay", 15*time.Minute, "Grace period for removal of unlinked files.")
	gcClientInterval := flag.Duration("gc-clients-interval", 24*time.Hour, "Client eviction interval (0 to disable).")
	clientExpiry := flag.Duration("client-expiry", 15*time.Minute, "Period of inactivity before a client is considered expired.")
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

	if *gcUnlinkedInterval != 0 {
		go func() {
			for {
				time.Sleep(*gcUnlinkedInterval)
				log.Printf("starting garbage collection of unlinked inodes")
				nRemoved, err := fs.RemoveExpiredUnlinked(hafs.RemoveExpiredUnlinkedOptions{
					RemovalDelay: *unlinkRemovalDelay,
				})
				log.Printf("garbage collection removed %d unlinked inodes", nRemoved)
				if err != nil {
					log.Printf("error removing unlinked inodes: %s", err)
				}
			}
		}()
	}

	if *gcClientInterval != 0 {
		go func() {
			for {
				time.Sleep(*gcClientInterval)
				log.Printf("starting garbage collection of expired clients")
				nEvicted, err := fs.EvictExpiredClients(hafs.EvictExpiredClientsOptions{
					ClientExpiry: *clientExpiry,
				})
				log.Printf("garbage collection evicted %d expired clients", nEvicted)
				if err != nil {
					log.Printf("error evicting expired clients: %s", err)
				}
			}
		}()
	}

	// Serve the file system, until unmounted by calling fusermount -u
	server.Wait()
}
