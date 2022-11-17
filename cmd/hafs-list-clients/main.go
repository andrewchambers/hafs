package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/andrewchambers/hafs"
	"github.com/andrewchambers/hafs/cli"
	"github.com/cheynewallace/tabby"
)

func main() {
	cli.RegisterClusterFileFlag()
	cli.RegisterFsNameFlag()
	flag.Parse()
	db := cli.MustOpenDatabase()

	clients, err := hafs.ListClients(db, cli.FsName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error listing clients: %s\n", err)
		os.Exit(1)
	}

	sort.Slice(clients, func(i, j int) bool { return clients[i].AttachTimeUnix > clients[j].AttachTimeUnix })

	t := tabby.New()
	t.AddHeader("ID", "DESCRIPTION", "HOSTNAME", "PID", "ATTACHED", "HEARTBEAT")
	for _, info := range clients {
		t.AddLine(
			info.Id,
			info.Description,
			info.Hostname,
			fmt.Sprintf("%d", info.Pid),
			time.Unix(int64(info.AttachTimeUnix), 0).Format(time.Stamp),
			time.Now().Sub(time.Unix(int64(info.HeartBeatUnix), 0)).Round(time.Second).String()+" ago",
		)
	}
	t.Print()
}
