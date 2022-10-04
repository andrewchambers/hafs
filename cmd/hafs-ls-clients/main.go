package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/andrewchambers/hafs/cli"
	"github.com/cheynewallace/tabby"
)

func main() {
	cli.RegisterDefaultFlags()
	flag.Parse()
	fs := cli.MustAttach()
	defer fs.Close()

	cli.RegisterDefaultSignalHandlers(fs)

	clients, err := fs.ListClients()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error listing clients: %s\n", err)
		os.Exit(1)
	}

	sort.Slice(clients, func(i, j int) bool { return clients[i].AttachTime.After(clients[j].AttachTime) })

	t := tabby.New()
	t.AddHeader("ID", "DESCRIPTION", "HOSTNAME", "PID", "ATTACHED", "HEARTBEAT")
	for _, info := range clients {
		t.AddLine(
			info.Id,
			info.Description,
			info.Hostname,
			fmt.Sprintf("%d", info.Pid),
			info.AttachTime.Format(time.Stamp),
			time.Now().Sub(info.HeartBeat).Round(time.Second).String()+" ago",
		)
	}
	t.Print()
}
