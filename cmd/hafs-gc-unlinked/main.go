package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/andrewchambers/hafs"
	"github.com/andrewchambers/hafs/cli"
)

func main() {
	verbose := flag.Bool("verbose", false, "Be verbose.")
	unlinkRemovalDelay := flag.Duration("unlink-removal-delay", 15*time.Minute, "Grace period for removal of unlinked files.")
	cli.RegisterClusterFileFlag()
	cli.RegisterFsNameFlag()
	flag.Parse()
	fs := cli.MustAttach(cli.MustOpenDatabase())
	defer fs.Close()

	cli.RegisterFsSignalHandlers(fs)

	nRemoved, err := fs.RemoveExpiredUnlinked(hafs.RemoveExpiredUnlinkedOptions{
		RemovalDelay: *unlinkRemovalDelay,
		OnRemoval: func(stat *hafs.Stat) {
			if *verbose {
				log.Printf("removing inode %d", stat.Ino)
			}
		},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error removing unlinked inodes: %s\n", err)
		os.Exit(1)
	}

	log.Printf("removed %d unlinked inodes\n", nRemoved)
}
