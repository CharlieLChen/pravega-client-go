package stream

import "io.pravega.pravega-client-go/controller"

type EventStreamWriter struct {
	streamName    string
	scope         string
	selector      *SegmentSelector
	controllerImp controller.Controller
}

func NewEventStreamWriter(scope, streamName string, controllerImp controller.Controller) *EventStreamWriter {
	selector := &SegmentSelector{
		controllerImp: controllerImp,
	}
	return &EventStreamWriter{
		selector:      selector,
		scope:         scope,
		streamName:    streamName,
		controllerImp: controllerImp,
	}
}
func (streamWriter *EventStreamWriter) writeEvent(event []byte, key string) error {
	segmentWriter, err := streamWriter.selector.chooseSegmentWriter(key)
	if err != nil {
		return err
	}
	err = segmentWriter.Write(event)
	if err != nil {
		return err
	}
	return nil
}
