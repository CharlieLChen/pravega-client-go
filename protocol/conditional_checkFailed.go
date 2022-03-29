package protocol

import (
	"github.com/google/uuid"
	"io"
	io_util "io.pravega.pravega-client-go/io"
)

// ==== ConditionalAppend
type ConditionalCheckFailed struct {
	/**
	  final UUID writerId;
	  final long eventNumber;
	  final long requestId;
	*/
	Type        *WireCommandType
	WriterId    uuid.UUID
	EventNumber int64
	RequestId   int64
}

func NewConditionalCheckFailed(writerId uuid.UUID, requestId, eventNumber int64) *ConditionalCheckFailed {
	return &ConditionalCheckFailed{
		Type:        TypeConditionalCheckFailed,
		RequestId:   requestId,
		WriterId:    writerId,
		EventNumber: eventNumber,
	}
}
func (conditionalCheckFailed *ConditionalCheckFailed) GetType() *WireCommandType {
	return TypeConditionalCheckFailed
}

func (conditionalCheckFailed *ConditionalCheckFailed) WriteFields(buffer *io_util.ByteBuffer) error {
	err := buffer.WriteUUid(conditionalCheckFailed.WriterId)
	if err != nil {
		return err
	}
	err = buffer.WriteInt64(conditionalCheckFailed.EventNumber)
	if err != nil {
		return err
	}

	err = buffer.WriteInt64(conditionalCheckFailed.RequestId)
	if err != nil {
		return err
	}
	return nil
}

type ConditionalConditionalCheckFailed struct {
}

func (c *ConditionalConditionalCheckFailed) ReadFrom(in io.Reader, length int32) (WireCommand, error) {

	writerId, err := io_util.ReadUUid(in)
	if err != nil {
		return nil, err
	}
	eventNumber, err := io_util.ReadInt64(in)
	if err != nil {
		return nil, err
	}
	requestId, err := io_util.ReadInt64(in)
	if err != nil {
		return nil, err
	}
	return NewConditionalCheckFailed(*writerId, requestId, eventNumber), nil
}
