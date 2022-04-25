package stream

import (
	"io.pravega.pravega-client-go/command"
	"io.pravega.pravega-client-go/connection"
	"io.pravega.pravega-client-go/controller"
	"io.pravega.pravega-client-go/protocol"
	"io.pravega.pravega-client-go/util"
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
func (streamWriter *EventStreamWriter) WriteEvent(event []byte, routineKey string) (*util.Future, error) {
	pendingEvent := protocol.NewPendingEvent(event, routineKey)
	streamWriter.selector.EventCha <- pendingEvent
	return pendingEvent.Future, nil
}
func (streamWriter *EventStreamWriter) Flush() {
	streamWriter.selector.CommandCha <- &command.Command{
		Code: command.Flush,
	}
	<-streamWriter.selector.CommandCha
}
