package protocol

import (
	"github.com/google/uuid"
	types "io.pravega.pravega-client-go/controller/proto"
)

type Append struct {
	Segment        *types.SegmentId
	WriterId       uuid.UUID
	EventNumber    int64
	EventCount     int64
	PendingEvent   *PendingEvent
	ExpectedLength *int64
	FlowId         int64
}

func NewAppend(segment *types.SegmentId, writeId uuid.UUID, eventNumber int64, pendingEvent *PendingEvent, flowId int64) *Append {

	return &Append{
		Segment:        segment,
		EventNumber:    eventNumber,
		PendingEvent:   pendingEvent,
		FlowId:         flowId,
		WriterId:       writeId,
		EventCount:     1,
		ExpectedLength: nil,
	}
}
