package hafs

import (
	"sync/atomic"
)

type atomicBool struct {
	v uint32
}

func (b *atomicBool) Store(v bool) {
	if v {
		atomic.StoreUint32(&b.v, 1)
	} else {
		atomic.StoreUint32(&b.v, 0)
	}
}

func (b *atomicBool) Load() bool {
	return atomic.LoadUint32(&b.v) == 1
}

type atomicUint64 struct {
	v uint64
}

func (b *atomicUint64) Add(n uint64) uint64 {
	return atomic.AddUint64(&b.v, n)
}

func (b *atomicUint64) Load() uint64 {
	return atomic.LoadUint64(&b.v)
}
