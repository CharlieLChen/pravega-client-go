package util

const (
	Nothing        = 'a'
	FutureComplete = int32(1)
)

type Future struct {
	result chan interface{}
}

func NewFuture() *Future {
	return &Future{
		result: make(chan interface{}, 1),
	}
}
func (future *Future) Get() interface{} {
	return <-future.result
}
func (future *Future) Complete(v interface{}) {
	future.result <- v
}
