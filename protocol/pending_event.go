package protocol

import "io.pravega.pravega-client-go/util"

type PendingEvent struct {
	Data       []byte
	Future     *util.Future
	RoutineKey string
}

func NewPendingEvent(data []byte, routineKey string) *PendingEvent {
	return &PendingEvent{
		Data:       data,
		RoutineKey: routineKey,
		Future:     util.NewFuture(),
	}
}
