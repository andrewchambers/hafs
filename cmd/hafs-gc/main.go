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
	unlinkRemovalDelay := flag.Duration("unlink-removal-delay", 15*time.Minute, "Grace period for removal of unlinked files.")
	clientTimeout := flag.Duration("client-timeout", 15*time.Minute, "Grace period for unresponsive clients.")
	cli.RegisterDefaultFlags()
	flag.Parse()
	fs := cli.MustAttach()
	defer fs.Close()

	cli.RegisterDefaultSignalHandlers(fs)

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
