package connection

import "sync/atomic"

var (
	IdGenerator = int32(0)
)

type Flow struct {
	FlowId                int32
	RequestSequenceNumber int32
}

func NewFlow() *Flow {
	flowId := atomic.AddInt32(&IdGenerator, 1)
	return &Flow{
		FlowId:                flowId,
		RequestSequenceNumber: 0,
	}
}

func (flow *Flow) AsLong() int64 {
	return int64((flow.FlowId << 32) | (flow.RequestSequenceNumber & 0xFFFFFFF))
}
