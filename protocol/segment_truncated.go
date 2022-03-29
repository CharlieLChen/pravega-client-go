package protocol

import (
	"io"
	io_util "io.pravega.pravega-client-go/io"
)

// ==== SegmentTruncated
type SegmentTruncated struct {
	Type      *WireCommandType
	RequestId int64
	Segment   string
}

func NewSegmentTruncated(requestId int64, segment string) *SegmentTruncated {
	return &SegmentTruncated{
		Type:      TypeSegmentTruncated,
		RequestId: requestId,
		Segment:   segment,
	}
}
func (segmentTruncated *SegmentTruncated) GetType() *WireCommandType {
	return TypeSegmentTruncated
}

func (segmentTruncated *SegmentTruncated) WriteFields(buffer *io_util.ByteBuffer) error {
	err := buffer.WriteInt64(segmentTruncated.RequestId)
	if err != nil {
		return err
	}
	err = buffer.WriteUTF(segmentTruncated.Segment)
	if err != nil {
		return err
	}
	return nil
}

func (segmentTruncated *SegmentTruncated) GetRequestId() int64 {
	return segmentTruncated.RequestId
}

func (segmentTruncated *SegmentTruncated) IsFailure() bool {
	return true
}

type SegmentTruncatedConstructor struct {
}

func (constructor *SegmentTruncatedConstructor) ReadFrom(in io.Reader, length int32) (WireCommand, error) {
	requestId, err := io_util.ReadInt64(in)
	if err != nil {
		return nil, err
	}
	segment, err := io_util.ReadUTF(in)
	if err != nil {
		return nil, err
	}

	return NewSegmentTruncated(requestId, segment), nil
}
