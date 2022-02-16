package event_wrap

import (
	"github.com/google/uuid"
)

type Append struct {
	Segment        string
	WriterId       uuid.UUID
	EventNumber    int64
	EventCount     int64
	Data           []byte
	ExpectedLength *int64
	FlowId         int64
}

func NewAppend(segment string, writeId uuid.UUID, eventNumber int64, data []byte, flowId int64) *Append {

	return &Append{
		Segment:        segment,
		EventNumber:    eventNumber,
		Data:           data,
		FlowId:         flowId,
		WriterId:       writeId,
		EventCount:     1,
		ExpectedLength: nil,
	}
}
