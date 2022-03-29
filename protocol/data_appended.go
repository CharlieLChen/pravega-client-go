package protocol

import (
	"github.com/google/uuid"
	"io"
	io_util "io.pravega.pravega-client-go/io"
)

// ==== APPEND_SETUP
type DataAppended struct {
	Type                      *WireCommandType
	RequestId                 int64
	WriterId                  uuid.UUID
	EventNumber               int64
	PreviousEventNumber       int64
	CurrentSegmentWriteOffset int64
}

func NewDataAppended(writerId uuid.UUID, requestId, eventNumber, previousEventNumber, currentSegmentWriteOffset int64) *DataAppended {
	return &DataAppended{
		Type:                      TypeAppendSetup,
		RequestId:                 requestId,
		WriterId:                  writerId,
		EventNumber:               eventNumber,
		PreviousEventNumber:       previousEventNumber,
		CurrentSegmentWriteOffset: currentSegmentWriteOffset,
	}
}
func (dataAppended *DataAppended) GetType() *WireCommandType {
	return TypeDataAppended
}

func (dataAppended *DataAppended) GetRequestId() int64 {
	return dataAppended.RequestId
}

func (dataAppended *DataAppended) IsFailure() bool {
	return false
}

func (dataAppended *DataAppended) WriteFields(buffer *io_util.ByteBuffer) error {
	err := buffer.WriteUUid(dataAppended.WriterId)
	if err != nil {
		return err
	}
	err = buffer.WriteInt64(dataAppended.EventNumber)
	if err != nil {
		return err
	}
	err = buffer.WriteInt64(dataAppended.PreviousEventNumber)
	if err != nil {
		return err
	}
	err = buffer.WriteInt64(dataAppended.RequestId)
	if err != nil {
		return err
	}
	err = buffer.WriteInt64(dataAppended.CurrentSegmentWriteOffset)
	if err != nil {
		return err
	}
	return nil
}

type DataAppendedConstructor struct {
}

func (constructor DataAppendedConstructor) ReadFrom(in io.Reader, length int32) (WireCommand, error) {
	writerId, err := io_util.ReadUUid(in)
	if err != nil {
		return nil, err
	}
	eventNumber, err := io_util.ReadInt64(in)
	if err != nil {
		return nil, err
	}
	previousEventNumber, err := io_util.ReadInt64(in)
	if err != nil {
		return nil, err
	}
	requestId, err := io_util.ReadInt64(in)
	if err != nil {
		return nil, err
	}
	currentSegmentWriteOffset, err := io_util.ReadInt64(in)
	if err != nil {
		return nil, err
	}

	return NewDataAppended(*writerId, requestId, eventNumber, previousEventNumber, currentSegmentWriteOffset), nil
}
