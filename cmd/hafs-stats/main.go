package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/andrewchambers/hafs/cli"
)

func main() {
	cli.RegisterDefaultFlags()
	flag.Parse()
	fs := cli.MustAttach()
	defer fs.Close()

	cli.RegisterDefaultSignalHandlers(fs)

	stats, err := fs.FsStats()

	if err != nil {
		fmt.Fprintf(os.Stderr, "error collecting stats: %s\n", err)
		os.Exit(1)
	}

	_, _ = fmt.Printf("FreeBytes: %d\n", stats.FreeBytes)
	_, _ = fmt.Printf("UsedBytes: %d\n", stats.UsedBytes)
}
