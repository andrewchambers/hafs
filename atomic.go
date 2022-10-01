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
