package protocol

import (
	"io"
	io_util "io.pravega.pravega-client-go/io"
)

// ==== SegmentRead
type SegmentRead struct {
	Type         *WireCommandType
	RequestId    int64
	Offset       int64
	Segment      string
	AtTail       bool
	EndOfSegment bool
	Data         []byte
}

func NewSegmentRead(segment string, requestId, offset int64, atTail, endOfSegment bool, data []byte) *SegmentRead {
	return &SegmentRead{
		Type:         TypeSegmentRead,
		RequestId:    requestId,
		Offset:       offset,
		Segment:      segment,
		AtTail:       atTail,
		EndOfSegment: endOfSegment,
		Data:         data,
	}
}
func (segmentRead *SegmentRead) GetType() *WireCommandType {
	return TypeSegmentRead
}

func (segmentRead *SegmentRead) WriteFields(buffer *io_util.ByteBuffer) error {
	err := buffer.WriteUTF(segmentRead.Segment)
	if err != nil {
		return err
	}
	err = buffer.WriteInt64(segmentRead.Offset)
	if err != nil {
		return err
	}
	err = buffer.WriteBool(segmentRead.AtTail)
	if err != nil {
		return err
	}
	err = buffer.WriteBool(segmentRead.EndOfSegment)
	if err != nil {
		return err
	}
	err = buffer.WriteInt32(int32(len(segmentRead.Data)))
	if err != nil {
		return err
	}
	err = buffer.Write(segmentRead.Data)
	if err != nil {
		return err
	}
	err = buffer.WriteInt64(segmentRead.RequestId)
	if err != nil {
		return err
	}
	return nil
}

func (segmentRead *SegmentRead) GetRequestId() int64 {
	return segmentRead.RequestId
}

func (segmentRead *SegmentRead) IsFailure() bool {
	return false
}

type SegmentReadConstructor struct {
}

func (constructor *SegmentReadConstructor) ReadFrom(in io.Reader, length int32) (WireCommand, error) {
	segment, err := io_util.ReadUTF(in)
	if err != nil {
		return nil, err
	}

	offset, err := io_util.ReadInt64(in)
	if err != nil {
		return nil, err
	}

	atTail, err := io_util.ReadBool(in)
	if err != nil {
		return nil, err
	}
	endOfSegment, err := io_util.ReadBool(in)
	if err != nil {
		return nil, err
	}

	length, err = io_util.ReadInt32(in)
	if err != nil {
		return nil, err
	}

	data, err := io_util.ReadWithLength(in, int(length))
	if err != nil {
		return nil, err
	}

	requestId, err := io_util.ReadInt64(in)
	if err != nil {
		return nil, err
	}

	return NewSegmentRead(segment, requestId, offset, atTail, endOfSegment, data), nil
}
