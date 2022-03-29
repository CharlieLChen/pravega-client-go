package protocol

import (
	"io"
	io_util "io.pravega.pravega-client-go/io"
)

// ==== NoSuchSegment
type NoSuchSegment struct {
	Type             *WireCommandType
	RequestId        int64
	Segment          string
	ServerStackTrace string
	Offset           int64
}

func NewNoSuchSegment(requestId int64, segment, serverStackTrace string, offset int64) *NoSuchSegment {
	return &NoSuchSegment{
		Type:             TypeWrongHost,
		RequestId:        requestId,
		Segment:          segment,
		ServerStackTrace: serverStackTrace,
		Offset:           offset,
	}
}
func (noSuchSegment *NoSuchSegment) GetType() *WireCommandType {
	return TypeNoSuchSegment
}

func (noSuchSegment *NoSuchSegment) WriteFields(buffer *io_util.ByteBuffer) error {
	err := buffer.WriteInt64(noSuchSegment.RequestId)
	if err != nil {
		return err
	}
	err = buffer.WriteUTF(noSuchSegment.Segment)
	if err != nil {
		return err
	}
	err = buffer.WriteUTF(noSuchSegment.ServerStackTrace)
	if err != nil {
		return err
	}
	err = buffer.WriteInt64(noSuchSegment.Offset)
	if err != nil {
		return err
	}
	return nil
}
func (noSuchSegment *NoSuchSegment) GetRequestId() int64 {
	return noSuchSegment.RequestId
}

func (noSuchSegment *NoSuchSegment) IsFailure() bool {
	return true
}

type NoSuchSegmentConstructor struct {
}

func (constructor *NoSuchSegmentConstructor) ReadFrom(in io.Reader, length int32) (WireCommand, error) {
	requestId, err := io_util.ReadInt64(in)
	if err != nil {
		return nil, err
	}
	segment, err := io_util.ReadUTF(in)
	if err != nil {
		return nil, err
	}

	serverStackTrace, err := io_util.ReadUTF(in)
	if err != nil {
		return nil, err
	}
	offset, err := io_util.ReadInt64(in)
	if err != nil {
		return nil, err
	}
	return NewNoSuchSegment(requestId, segment, serverStackTrace, offset), nil
}
