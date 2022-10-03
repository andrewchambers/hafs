package main

import (
	"log"
	"os"
	"sync"
	"syscall"
)

func main() {
	// Abuse posix locks by opening and closing the same inode from the same process.
	// Normally this doesn't make any sense, but we are stress testing hafs to ensure
	// no locks get dropped.
	for {
		wg := &sync.WaitGroup{}
		start := make(chan struct{})
		for i := 0; i < 10; i += 1 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = <-start
				f, err := os.Create("foo.lock")
				if err != nil {
					log.Fatalf("error opening file: %s", err)
				}
				defer f.Close()

				flockT := syscall.Flock_t{
					Type:   syscall.F_WRLCK,
					Whence: 0,
					Start:  0,
					Len:    0,
				}
				err = syscall.FcntlFlock(f.Fd(), syscall.F_SETLKW, &flockT)
				if err != nil {
					log.Printf("could not acquire lock - %s.", err)
				} else {
					log.Print("lock acquired.")
				}
			}()
		}
		close(start)
		wg.Wait()
	}
}
