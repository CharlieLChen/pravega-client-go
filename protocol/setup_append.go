package protocol

import (
	"github.com/google/uuid"
	"io"
	io_util "io.pravega.pravega-client-go/io"
)

// ==== SetupAppend
type SetupAppend struct {
	Type            *WireCommandType
	RequestId       int64
	WriterId        uuid.UUID
	Segment         string
	DelegationToken string
}

func NewSetupAppend(requestId int64, writerId uuid.UUID, segment string, delegationToken string) *SetupAppend {
	return &SetupAppend{
		Type:            TypeSetupAppend,
		RequestId:       requestId,
		WriterId:        writerId,
		Segment:         segment,
		DelegationToken: delegationToken,
	}
}
func (setup *SetupAppend) GetType() *WireCommandType {
	return TypeSetupAppend
}

func (setup *SetupAppend) WriteFields(buffer *io_util.ByteBuffer) error {
	err := buffer.WriteInt64(setup.RequestId)
	if err != nil {
		return err
	}
	binary, err := setup.WriterId.MarshalBinary()
	if err != nil {
		return err
	}
	err = buffer.Write(binary)
	if err != nil {
		return err
	}
	err = buffer.WriteUTF(setup.Segment)
	if err != nil {
		return err
	}
	err = buffer.WriteUTF(setup.DelegationToken)
	if err != nil {
		return err
	}
	return nil
}

type SetupAppendConstructor struct {
}

func (setup *SetupAppendConstructor) ReadFrom(in io.Reader, length int32) (WireCommand, error) {
	requestId, err := io_util.ReadInt64(in)
	if err != nil {
		return nil, err
	}

	writerId, err := io_util.ReadUUid(in)
	if err != nil {
		return nil, err
	}

	segment, err := io_util.ReadUTF(in)
	if err != nil {
		return nil, err
	}

	token, err := io_util.ReadUTF(in)
	if err != nil {
		return nil, err
	}
	return NewSetupAppend(requestId, *writerId, segment, token), nil
}
