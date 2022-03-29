package protocol

import (
	"io"
	io_util "io.pravega.pravega-client-go/io"
)

// ==== ReadSegment
type ReadSegment struct {
	Type            *WireCommandType
	RequestId       int64
	Offset          int64
	SuggestedLength int64
	Segment         string
	DelegationToken string
}

func NewReadSegment(segment, delegationToken string, requestId, offset, suggestedLength int64) *ReadSegment {
	return &ReadSegment{
		Type:            TypeSetupAppend,
		RequestId:       requestId,
		Offset:          offset,
		Segment:         segment,
		DelegationToken: delegationToken,
		SuggestedLength: suggestedLength,
	}
}
func (readSegment *ReadSegment) GetType() *WireCommandType {
	return TypeReadSegment
}

func (readSegment *ReadSegment) WriteFields(buffer *io_util.ByteBuffer) error {
	err := buffer.WriteUTF(readSegment.Segment)
	if err != nil {
		return err
	}
	err = buffer.WriteInt64(readSegment.Offset)
	if err != nil {
		return err
	}
	err = buffer.WriteInt64(readSegment.SuggestedLength)
	if err != nil {
		return err
	}
	err = buffer.WriteUTF(readSegment.DelegationToken)
	if err != nil {
		return err
	}
	err = buffer.WriteInt64(readSegment.RequestId)
	if err != nil {
		return err
	}
	return nil
}

type ReadSegmentConstructor struct {
}

func (setup *ReadSegmentConstructor) ReadFrom(in io.Reader, length int32) (WireCommand, error) {
	segment, err := io_util.ReadUTF(in)
	if err != nil {
		return nil, err
	}

	offset, err := io_util.ReadInt64(in)
	if err != nil {
		return nil, err
	}

	suggestedLength, err := io_util.ReadInt64(in)
	if err != nil {
		return nil, err
	}

	token, err := io_util.ReadUTF(in)
	if err != nil {
		return nil, err
	}
	requestId, err := io_util.ReadInt64(in)
	if err != nil {
		return nil, err
	}

	return NewReadSegment(segment, token, requestId, offset, suggestedLength), nil
}
