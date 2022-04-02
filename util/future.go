package util

import (
	log "github.com/sirupsen/logrus"
	"sync/atomic"
	"unsafe"
)

const (
	Nothing        = 'a'
	FutureComplete = int32(1)
)

type Future struct {
	result   *interface{}
	complete int32
}

func NewFuture() *Future {
	return &Future{}
}
func (future *Future) Get() interface{} {
	for atomic.LoadInt32(&future.complete) != FutureComplete {
	}
	atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(future.result)))
	return *future.result
}
func (future *Future) Complete(v interface{}) {
	atomic.SwapPointer((*unsafe.Pointer)(unsafe.Pointer(&future.result)), unsafe.Pointer(&v))
	log.Printf("%v", future.result)
	atomic.SwapInt32(&future.complete, FutureComplete)
}
