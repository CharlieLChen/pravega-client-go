package protocol

import (
	"io"
	io_util "io.pravega.pravega-client-go/io"
)

// ==== SetupAppend
type SegmentExists struct {
	Type             *WireCommandType
	RequestId        int64
	Segment          string
	ServerStackTrace string
}

func NewSegmentExists(requestId int64, segment, serverStackTrace string) *SegmentExists {
	return &SegmentExists{
		Type:             TypeSegmentExists,
		RequestId:        requestId,
		Segment:          segment,
		ServerStackTrace: serverStackTrace,
	}
}
func (segmentExists *SegmentExists) GetType() *WireCommandType {
	return TypeSegmentExists
}

func (segmentExists *SegmentExists) WriteFields(buffer *io_util.ByteBuffer) error {
	err := buffer.WriteInt64(segmentExists.RequestId)
	if err != nil {
		return err
	}
	err = buffer.WriteUTF(segmentExists.Segment)
	if err != nil {
		return err
	}
	err = buffer.WriteUTF(segmentExists.ServerStackTrace)
	if err != nil {
		return err
	}
	return nil
}
func (segmentExists *SegmentExists) GetRequestId() int64 {
	return segmentExists.RequestId
}

func (segmentExists *SegmentExists) IsFailure() bool {
	return true
}

type SegmentExistsConstructor struct {
}

func (constructor *SegmentExistsConstructor) ReadFrom(in io.Reader, length int32) (WireCommand, error) {
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
	return NewSegmentExists(requestId, segment, serverStackTrace), nil
}
