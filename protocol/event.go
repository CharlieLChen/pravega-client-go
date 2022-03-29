package protocol

import (
	"io"
	io_util "io.pravega.pravega-client-go/io"
)

// ==== EVENT
type Event struct {
	Type *WireCommandType
	Data []byte
}

func NewEvent(data []byte) *Event {
	return &Event{
		Type: TypeEvent,
		Data: data,
	}
}

func (event *Event) GetType() *WireCommandType {
	return TypeEvent
}

func (event *Event) WriteFields(buffer *io_util.ByteBuffer) error {
	err := buffer.Write(event.Data)
	if err != nil {
		return err
	}
	return nil
}

func (event *Event) GetEncodedData() ([]byte, error) {
	buffer := io_util.NewByteBuffer(0)
	err := buffer.WriteInt32(TypeEvent.Code)
	if err != nil {
		return nil, err
	}
	length := len(event.Data)
	err = buffer.WriteInt32(int32(length))
	if err != nil {
		return nil, err
	}
	err = buffer.Write(event.Data)
	if err != nil {
		return nil, err
	}
	return buffer.Data(), nil
}

type EventConstructor struct {
}

func (constructor *EventConstructor) ReadFrom(in io.Reader, length int32) (WireCommand, error) {
	data, err := io_util.ReadWithLength(in, int(length))
	if err != nil {
		return nil, err
	}
	return NewEvent(data), nil
}
