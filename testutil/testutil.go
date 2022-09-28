package testutil

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func GetFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

type FDBTestServer struct {
	t           *testing.T
	ClusterFile string
	fdbServer   *exec.Cmd
}

// Create a test server that is automatically cleaned up when the test finishes.
func NewFDBTestServer(t *testing.T) *FDBTestServer {
	_, err := exec.LookPath("fdbserver")
	if err != nil {
		t.Skip("fdbserver not found in path")
	}

	port, err := GetFreePort()
	if err != nil {
		t.Fatal(err)
	}
	listenAddress := fmt.Sprintf("127.0.0.1:%d", port)

	dir := t.TempDir()
	clusterFile := filepath.Join(dir, "fdb.cluster")

	err = os.WriteFile(clusterFile, []byte(fmt.Sprintf("testcluster:12345678@%s", listenAddress)), 0o644)
	if err != nil {
		t.Fatal(err)
	}

	fdbServerOpts := []string{
		"-p", listenAddress,
		"-C", clusterFile,
	}

	fdbServer := exec.Command(
		"fdbserver",
		fdbServerOpts...,
	)
	fdbServer.Dir = dir

	rpipe, wpipe, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}

	logWg := &sync.WaitGroup{}
	logWg.Add(1)
	go func() {
		defer logWg.Done()
		brdr := bufio.NewReader(rpipe)
		for {
			line, err := brdr.ReadString('\n')
			if err != nil {
				return
			}
			if len(line) == 0 {
				continue
			}
			t.Logf("fdbserver: %s", line[:len(line)-1])
		}
	}()

	t.Cleanup(func() {
		logWg.Wait()
	})

	fdbServer.Stderr = wpipe
	fdbServer.Stdout = wpipe

	err = fdbServer.Start()
	if err != nil {
		t.Fatal(err)
	}
	_ = wpipe.Close()

	t.Cleanup(func() {
		_ = fdbServer.Process.Signal(syscall.SIGTERM)
		_, _ = fdbServer.Process.Wait()
	})

	t.Logf("starting fdbserver %v", fdbServerOpts)

	t.Logf("creating cluster...")
	err = exec.Command(
		"fdbcli",
		[]string{
			"-C", clusterFile,
			"--exec", "configure new single memory",
		}...,
	).Run()
	if err != nil {
		t.Fatalf("unable to configure new cluster: %s", err)
	}

	up := false
	for i := 0; i < 2000; i++ {
		c, err := net.Dial("tcp", listenAddress)
		if err == nil {
			up = true
			_ = c.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !up {
		t.Fatal("fdb server never came up")
	}

	return &FDBTestServer{
		t:           t,
		ClusterFile: clusterFile,
		fdbServer:   fdbServer,
	}
}

func (fdbServer *FDBTestServer) Dial() fdb.Database {
	db := fdb.MustOpenDatabase(fdbServer.ClusterFile)
	return db
}
