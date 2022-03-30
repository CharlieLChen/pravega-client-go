package stream

import (
	"io.pravega.pravega-client-go/connection"
	"io.pravega.pravega-client-go/controller"
)

type EventStreamWriter struct {
	streamName    string
	scope         string
	selector      *SegmentSelector
	controllerImp *controller.ControllerImpl
	sockets       *connection.Sockets
	dispatcher    *connection.ResponseDispatcher
}

func NewEventStreamWriter(scope, streamName string, controllerImp *controller.ControllerImpl, sockets *connection.Sockets) *EventStreamWriter {
	selector := NewSegmentSelector(scope, streamName, controllerImp, sockets)
	return &EventStreamWriter{
		selector:      selector,
		scope:         scope,
		streamName:    streamName,
		controllerImp: controllerImp,
		sockets:       sockets,
	}
}
func (streamWriter *EventStreamWriter) WriteEvent(event []byte, routineKey string) error {
	segmentWriter, err := streamWriter.selector.chooseSegmentWriter(routineKey)
	if err != nil {
		return err
	}
	segmentWriter.WriteCh <- event

	return nil
}
