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
	cli.RegisterClusterFileFlag()
	cli.RegisterFsNameFlag()
	flag.Parse()
	db := cli.MustOpenDatabase()

	nEvicted, err := hafs.EvictExpiredClients(db, cli.FsName, hafs.EvictExpiredClientsOptions{
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

	log.Printf("evicted %d expired clients\n", nEvicted)
}
