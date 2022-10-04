package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/andrewchambers/hafs"
	"github.com/andrewchambers/hafs/cli"
	"golang.org/x/sys/unix"
)

func main() {
	unlinkRemovalDelay := flag.Duration("unlink-removal-delay", 6*time.Hour, "Grace period for removal of unlinked files.")
	clientTimeout := flag.Duration("client-timeout", 6*time.Hour, "Grace period for unresponsive clients.")
	flag.Parse()
	fs := cli.MustAttach()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, unix.SIGINT, unix.SIGTERM)

	go func() {
		<-sigChan
		signal.Reset()
		fmt.Fprintf(os.Stderr, "closing down due to signal...\n")
		err := fs.Close()
		if err != nil {
			fmt.Fprintf(os.Stderr, "error disconnecting client: %s\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}()

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
