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
	clientExpiry := flag.Duration("client-expiry", 15*time.Minute, "Period of inactivity before a client is considered expired.")
	cli.RegisterDefaultFlags()
	flag.Parse()
	fs := cli.MustAttach()
	defer fs.Close()

	cli.RegisterDefaultSignalHandlers(fs)

	nEvicted, err := fs.EvictExpiredClients(hafs.EvictExpiredClientsOptions{
		ClientExpiry: *clientExpiry,
		OnEviction: func(clientId string) {
			if *verbose {
				log.Printf("evicting client %s", clientId)
			}
		},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error removing evicting expired clients: %s\n", err)
		os.Exit(1)
	}

	if *verbose {
		log.Printf("evicted %d expired clients\n", nEvicted)
	}

}
