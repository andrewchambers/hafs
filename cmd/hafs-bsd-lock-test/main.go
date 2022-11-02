package main

import (
	"os"
	"runtime"
	"syscall"
	"time"
)

func checkBSDLocks() {
	f1, err := os.Create("f.lck")
	if err != nil {
		panic(err)
	}
	f2, err := os.Open("f.lck")
	if err != nil {
		panic(err)
	}
	f3, err := os.Open("f.lck")
	if err != nil {
		panic(err)
	}
	err = syscall.Flock(int(f1.Fd()), syscall.LOCK_SH|syscall.LOCK_NB)
	if err != nil {
		panic(err)
	}
	err = syscall.Flock(int(f2.Fd()), syscall.LOCK_SH|syscall.LOCK_NB)
	if err != nil {
		panic(err)
	}
	err = syscall.Flock(int(f3.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err == nil {
		panic(err)
	}
	err = syscall.Flock(int(f1.Fd()), syscall.LOCK_UN|syscall.LOCK_NB)
	if err != nil {
		panic(err)
	}
	err = syscall.Flock(int(f3.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err == nil {
		panic(err)
	}
	_ = f2.Close()
	// XXX Fuse release seems to be done asynchronously - we must wait before we can see the unlock.
	time.Sleep(10 * time.Millisecond)
	err = syscall.Flock(int(f3.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		panic(err)
	}
	err = syscall.Flock(int(f1.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err == nil {
		panic(err)
	}
	_ = f3.Close()
	time.Sleep(10 * time.Millisecond)
	err = syscall.Flock(int(f1.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		panic(err)
	}
	dupedFd, err := syscall.Dup(int(f1.Fd()))
	if err != nil {
		panic(err)
	}
	f2 = os.NewFile(uintptr(dupedFd), "duped")
	f3, err = os.Open("f.lck")
	if err != nil {
		panic(err)
	}
	err = syscall.Flock(int(f3.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err == nil {
		panic(err)
	}
	_ = f1.Close()
	time.Sleep(10 * time.Millisecond)
	err = syscall.Flock(int(f3.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err == nil {
		panic(err)
	}
	_ = f2.Close()
	time.Sleep(10 * time.Millisecond)
	err = syscall.Flock(int(f3.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		panic(err)
	}
}

func main() {
	runtime.LockOSThread()
	checkBSDLocks()
}
