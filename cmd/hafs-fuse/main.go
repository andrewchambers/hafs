package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"time"

	"github.com/andrewchambers/hafs"
	"github.com/andrewchambers/hafs/cli"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func usage() {
	fmt.Printf("hafs-fuse [OPTS] MOUNTPOINT\n\n")
	flag.Usage()
	os.Exit(1)
}

func main() {
	cli.RegisterClusterFileFlag()
	cli.RegisterClientDescriptionFlag()
	cli.RegisterFsNameFlag()
	cli.RegisterSmallObjectOptimizationThresholdFlag()
	debugFuse := flag.Bool("debug-fuse", false, "Log fuse messages.")
	readdirPlus := flag.Bool("readdir-plus", false, "Enable readdir plus when listing directories (stat and readdir calls are batched together).")
	gcUnlinkedInterval := flag.Duration("gc-unlinked-interval", 8*time.Hour, "Unlinked inode garbage collection interval (0 to disable).")
	unlinkRemovalDelay := flag.Duration("unlink-removal-delay", 15*time.Minute, "Grace period for removal of unlinked files.")
	gcClientInterval := flag.Duration("gc-clients-interval", 24*time.Hour, "Client eviction interval (0 to disable).")
	clientExpiry := flag.Duration("client-expiry", 15*time.Minute, "Period of inactivity before a client is considered expired.")
	cacheDentries := flag.Duration("cache-dentries", 0, "Duration to cache dentry lookups, use with great care.")
	cacheAttributes := flag.Duration("cache-attributes", 0, "Duration to cache file attribute lookups, use with great care.")
	notifyCommand := flag.String("notify-command", "", "A command to run via sh -c \"$CMD\" once filesystem is successfully mounted.")

	flag.Parse()

	if len(flag.Args()) != 1 {
		usage()
	}

	mntDir := flag.Args()[0]

	db := cli.MustOpenDatabase()
	fs := cli.MustAttach(db)
	defer fs.Close()

	cli.RegisterFsSignalHandlers(fs)

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
			DisableReadDirPlus:   !*readdirPlus,
			MaxWrite:             fuse.MAX_KERNEL_WRITE,
			MaxReadAhead:         fuse.MAX_KERNEL_WRITE, // XXX Use the max write as a guide for now, is this good?
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
	log.Printf("filesystem successfully mounted")

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
				nEvicted, err := hafs.EvictExpiredClients(db, cli.FsName, hafs.EvictExpiredClientsOptions{
					ClientExpiry: *clientExpiry,
				})
				log.Printf("garbage collection evicted %d expired clients", nEvicted)
				if err != nil {
					log.Printf("error evicting expired clients: %s", err)
				}
			}
		}()
	}

	if *notifyCommand != "" {
		cmdOut, err := exec.Command("sh", "-c", *notifyCommand).CombinedOutput()
		if err != nil {
			log.Fatalf("error running notify command: %s, output: %q", err, string(cmdOut))
		}
	}

	// Serve the file system, until unmounted by calling fusermount -u
	server.Wait()
}
