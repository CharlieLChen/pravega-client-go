package protocol

import (
	"github.com/google/uuid"
	"io"
	io_util "io.pravega.pravega-client-go/io"
)

// ==== ConditionalAppend
type ConditionalAppend struct {
	Type           *WireCommandType
	WriterId       uuid.UUID
	EventNumber    int64
	ExpectedOffset int64
	Event          *Event
	RequestId      int64
}

func NewConditionalAppend(requestId int64, writerId uuid.UUID, eventNumber int64, expectedOffset int64, event *Event) *ConditionalAppend {
	return &ConditionalAppend{
		Type:           TypeSetupAppend,
		RequestId:      requestId,
		WriterId:       writerId,
		EventNumber:    eventNumber,
		Event:          event,
		ExpectedOffset: expectedOffset,
	}
}
func (conditionalAppend *ConditionalAppend) GetType() *WireCommandType {
	return TypeConditionalAppend
}

func (conditionalAppend *ConditionalAppend) WriteFields(buffer *io_util.ByteBuffer) error {
	err := buffer.WriteUUid(conditionalAppend.WriterId)
	if err != nil {
		return err
	}
	err = buffer.WriteInt64(conditionalAppend.EventNumber)
	if err != nil {
		return err
	}
	err = buffer.WriteInt64(conditionalAppend.ExpectedOffset)
	if err != nil {
		return err
	}
	data, err := conditionalAppend.Event.GetEncodedData()
	if err != nil {
		return err
	}
	err = buffer.Write(data)
	if err != nil {
		return err
	}

	err = buffer.WriteInt64(conditionalAppend.RequestId)
	if err != nil {
		return err
	}
	return nil
}

type ConditionalAppendConstructor struct {
}

func (conditionalAppend *ConditionalAppendConstructor) ReadFrom(in io.Reader, length int32) (WireCommand, error) {

	writerId, err := io_util.ReadUUid(in)
	if err != nil {
		return nil, err
	}
	eventNumber, err := io_util.ReadInt64(in)
	if err != nil {
		return nil, err
	}
	expectedOffset, err := io_util.ReadInt64(in)
	if err != nil {
		return nil, err
	}
	e, err := Decode(in)
	event := e.(*Event)
	if err != nil {
		return nil, err
	}

	requestId, err := io_util.ReadInt64(in)
	if err != nil {
		return nil, err
	}
	return NewConditionalAppend(requestId, *writerId, eventNumber, expectedOffset, event), nil
}
