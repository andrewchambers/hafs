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

func (b *atomicUint64) CompareAndSwap(old, new uint64) bool {
	return atomic.CompareAndSwapUint64(&b.v, old, new)
}

type AtomicBitset []atomicUint64

func NewAtomicBitset(size uint) AtomicBitset {
	size = (size + 63) &^ 63
	return make(AtomicBitset, size/64)
}

func (a AtomicBitset) index(bit uint) (ptr *atomicUint64, mask uint64) {
	return &a[bit/64], 1 << (bit & 63)
}

func (a AtomicBitset) Set(bit uint) {
	ptr, mask := a.index(bit)
	old := ptr.Load()
	for !ptr.CompareAndSwap(old, old|mask) {
		old = ptr.Load()
	}
}

func (a AtomicBitset) IsSet(bit uint) bool {
	ptr, mask := a.index(bit)
	return ptr.Load()&mask != 0
}
