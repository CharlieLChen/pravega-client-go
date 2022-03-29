package protocol

import (
	"io"
	io_util "io.pravega.pravega-client-go/io"
)

// ==== SetupAppend
type SegmentSealed struct {
	Type             *WireCommandType
	RequestId        int64
	Segment          string
	ServerStackTrace string
	Offset           int64
}

func NewSegmentSealed(requestId int64, segment, serverStackTrace string, offset int64) *SegmentSealed {
	return &SegmentSealed{
		Type:             TypeWrongHost,
		RequestId:        requestId,
		Segment:          segment,
		ServerStackTrace: serverStackTrace,
		Offset:           offset,
	}
}
func (segmentSealed *SegmentSealed) GetType() *WireCommandType {
	return TypeSegmentSealed
}

func (segmentSealed *SegmentSealed) WriteFields(buffer *io_util.ByteBuffer) error {
	err := buffer.WriteInt64(segmentSealed.RequestId)
	if err != nil {
		return err
	}
	err = buffer.WriteUTF(segmentSealed.Segment)
	if err != nil {
		return err
	}
	err = buffer.WriteUTF(segmentSealed.ServerStackTrace)
	if err != nil {
		return err
	}
	err = buffer.WriteInt64(segmentSealed.Offset)
	if err != nil {
		return err
	}
	return nil
}

func (segmentSealed *SegmentSealed) GetRequestId() int64 {
	return segmentSealed.RequestId
}

func (segmentSealed *SegmentSealed) IsFailure() bool {
	return true
}

type SegmentSealedConstructor struct {
}

func (constructor *SegmentSealedConstructor) ReadFrom(in io.Reader, length int32) (WireCommand, error) {
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
	return NewSegmentSealed(requestId, segment, serverStackTrace, offset), nil
}
