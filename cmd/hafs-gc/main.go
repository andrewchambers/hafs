package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/andrewchambers/hafs"
	"github.com/andrewchambers/hafs/cli"
)

func main() {
	unlinkRemovalDelay := flag.Duration("unlink-removal-delay", 6*time.Hour, "Grace period for removal of unlinked files.")
	clientTimeout := flag.Duration("client-timeout", 6*time.Hour, "Grace period for unresponsive clients.")
	flag.Parse()
	fs := cli.MustAttach()

	stats, err := fs.CollectGarbage(hafs.CollectGarbageOpts{
		UnlinkedRemovalDelay: *unlinkRemovalDelay,
		ClientTimeout:        *clientTimeout,
	})

	_, _ = fmt.Printf("UnlinkedRemovalCount: %d\n", stats.UnlinkedRemovalCount)
	_, _ = fmt.Printf("ClientEvictionCount: %d\n", stats.ClientEvictionCount)

	if err != nil {
		fmt.Fprintf(os.Stderr, "error collecting garbage: %s\n", err)
		os.Exit(1)
	}
}
