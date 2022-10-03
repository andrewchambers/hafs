package main

import (
	"os"
	"syscall"
)

func main() {

	f1, err := os.Create("foo.lock")
	if err != nil {
		panic(err)
	}
	f2, err := os.Create("foo.lock")
	if err != nil {
		panic(err)
	}

	flockT := syscall.Flock_t{
		Type:   syscall.F_WRLCK,
		Whence: 0,
		Start:  0,
		Len:    0,
	}

	err = syscall.FcntlFlock(f1.Fd(), syscall.F_SETLKW, &flockT)
	if err != nil {
		panic(err)
	}

	_ = f1.Close()

	err = syscall.FcntlFlock(f2.Fd(), syscall.F_SETLKW, &flockT)
	if err != nil {
		panic(err)
	}
}
