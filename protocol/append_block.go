package protocol

import (
	"github.com/google/uuid"
	"io"
	io_util "io.pravega.pravega-client-go/io"
)

// ==== APPEND_BLOCK
type AppendBlock struct {
	Type     *WireCommandType
	WriterId uuid.UUID
}

func NewAppendBlock(writerId uuid.UUID) *AppendBlock {
	return &AppendBlock{
		WriterId: writerId,
	}
}
func (appendBlock *AppendBlock) GetType() *WireCommandType {
	return TypeAppendBlock
}

func (appendBlock *AppendBlock) WriteFields(buffer *io_util.ByteBuffer) error {
	binary, err := appendBlock.WriterId.MarshalBinary()
	if err != nil {
		return err
	}
	err = buffer.Write(binary)
	if err != nil {
		return err
	}
	return nil
}

type AppendBlockConstructor struct {
}

func (appendBlock AppendBlockConstructor) ReadFrom(in io.Reader, length int32) (WireCommand, error) {
	uid, err := io_util.ReadUUid(in)
	if err != nil {
		return nil, err
	}
	return NewAppendBlock(*uid), nil
}

type AppendBlockEnd struct {
	Type              *WireCommandType
	WriterId          uuid.UUID
	SizeOfWholeEvents int32
	PartialData       []byte
	NumEvents         int32
	LastEventNumber   int64
	RequestId         int64
}

func NewAppendBlockEnd(writerId uuid.UUID, sizeOfWholeEvents int32, numEvents int32, lastEventNumber int64, requestId int64) *AppendBlockEnd {
	return &AppendBlockEnd{
		WriterId:          writerId,
		SizeOfWholeEvents: sizeOfWholeEvents,
		NumEvents:         numEvents,
		LastEventNumber:   lastEventNumber,
		RequestId:         requestId,
	}
}

func (appendBlockEnd *AppendBlockEnd) GetType() *WireCommandType {
	return TypeAppendBlockEnd
}

func (appendBlockEnd *AppendBlockEnd) WriteFields(buffer *io_util.ByteBuffer) error {
	err := buffer.WriteUUid(appendBlockEnd.WriterId)
	if err != nil {
		return err
	}
	err = buffer.WriteInt32(appendBlockEnd.SizeOfWholeEvents)
	if err != nil {
		return err
	}
	err = buffer.WriteInt32(int32(0)) //never has partial data
	if err != nil {
		return err
	}
	err = buffer.WriteInt32(appendBlockEnd.NumEvents)
	if err != nil {
		return err
	}
	err = buffer.WriteInt64(appendBlockEnd.LastEventNumber)
	if err != nil {
		return err
	}
	err = buffer.WriteInt64(appendBlockEnd.RequestId)
	if err != nil {
		return err
	}
	return nil
}

type AppendBlockEndConstructor struct {
}

func (appendBlockEnd AppendBlockEndConstructor) ReadFrom(in io.Reader, length int32) (WireCommand, error) {
	uid, err := io_util.ReadUUid(in)
	if err != nil {
		return nil, err
	}

	sizeOfWholeEvents, err := io_util.ReadInt32(in)
	if err != nil {
		return nil, err
	}
	_, err = io_util.ReadInt32(in)
	if err != nil {
		return nil, err
	}
	numEvents, err := io_util.ReadInt32(in)
	if err != nil {
		return nil, err
	}
	lastEventNumber, err := io_util.ReadInt64(in)
	if err != nil {
		return nil, err
	}
	requestId, err := io_util.ReadInt64(in)
	if err != nil {
		return nil, err
	}
	return NewAppendBlockEnd(*uid, sizeOfWholeEvents, numEvents, lastEventNumber, requestId), nil
}
