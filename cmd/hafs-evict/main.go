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

	args := flag.Args()
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "expecting a single client to evict\n")
		os.Exit(1)
	}

	err := fs.EvictClient(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "error evicting client: %s\n", err)
		os.Exit(1)
	}

}
