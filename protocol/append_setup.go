package protocol

import (
	"github.com/google/uuid"
	"io"
	io_util "io.pravega.pravega-client-go/io"
)

// ==== APPEND_SETUP
type AppendSetup struct {
	Type            *WireCommandType
	RequestId       int64
	WriterId        uuid.UUID
	Segment         string
	LastEventNumber int64
}

func NewAppendSetup(requestId int64, writerId uuid.UUID, segment string, lastEventNumber int64) *AppendSetup {
	return &AppendSetup{
		Type:            TypeAppendSetup,
		RequestId:       requestId,
		WriterId:        writerId,
		Segment:         segment,
		LastEventNumber: lastEventNumber,
	}
}
func (appendSetup *AppendSetup) GetType() *WireCommandType {
	return TypeAppendSetup
}

func (appendSetup *AppendSetup) GetRequestId() int64 {
	return appendSetup.RequestId
}

func (appendSetup *AppendSetup) IsFailure() bool {
	return false
}

type AppendSetupConstructor struct {
}

func (appendSetup *AppendSetup) WriteFields(buffer *io_util.ByteBuffer) error {

	err := buffer.WriteInt64(appendSetup.RequestId)
	if err != nil {
		return err
	}
	err = buffer.WriteUTF(appendSetup.Segment)
	if err != nil {
		return err
	}
	err = buffer.WriteUUid(appendSetup.WriterId)
	if err != nil {
		return err
	}
	err = buffer.WriteInt64(appendSetup.LastEventNumber)
	if err != nil {
		return err
	}
	return nil
}

func (constructor AppendSetupConstructor) ReadFrom(in io.Reader, length int32) (WireCommand, error) {
	requestId, err := io_util.ReadInt64(in)
	if err != nil {
		return nil, err
	}
	utf, err := io_util.ReadUTF(in)
	if err != nil {
		return nil, err
	}
	writerId, err := io_util.ReadUUid(in)
	if err != nil {
		return nil, err
	}
	lastEventNumber, err := io_util.ReadInt64(in)
	if err != nil {
		return nil, err
	}
	return NewAppendSetup(requestId, *writerId, utf, lastEventNumber), nil
}
