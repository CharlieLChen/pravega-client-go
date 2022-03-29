package protocol

import (
	"github.com/google/uuid"
	"io"
	io_util "io.pravega.pravega-client-go/io"
)

// ==== InvalidEventNumber
type InvalidEventNumber struct {
	Type             *WireCommandType
	WriterId         uuid.UUID
	ServerStackTrace string
	EventNumber      int64
}

func NewInvalidEventNumber(writerId uuid.UUID, eventNumber int64, serverStackTrace string) *InvalidEventNumber {
	return &InvalidEventNumber{
		Type:             TypeInvalidEventNumber,
		EventNumber:      eventNumber,
		WriterId:         writerId,
		ServerStackTrace: serverStackTrace,
	}
}
func (invalidEventNumber *InvalidEventNumber) GetType() *WireCommandType {
	return TypeInvalidEventNumber
}

func (invalidEventNumber *InvalidEventNumber) WriteFields(buffer *io_util.ByteBuffer) error {
	err := buffer.WriteUUid(invalidEventNumber.WriterId)
	if err != nil {
		return err
	}
	err = buffer.WriteInt64(invalidEventNumber.EventNumber)
	if err != nil {
		return err
	}
	err = buffer.WriteUTF(invalidEventNumber.ServerStackTrace)
	if err != nil {
		return err
	}

	return nil
}

func (invalidEventNumber *InvalidEventNumber) GetRequestId() int64 {
	// !!!!!!! Don't ask me why.
	return invalidEventNumber.EventNumber
}

func (invalidEventNumber *InvalidEventNumber) IsFailure() bool {
	return true
}

type InvalidEventNumberConstructor struct {
}

func (constructor *InvalidEventNumberConstructor) ReadFrom(in io.Reader, length int32) (WireCommand, error) {
	writerId, err := io_util.ReadUUid(in)
	if err != nil {
		return nil, err
	}
	eventNumber, err := io_util.ReadInt64(in)
	if err != nil {
		return nil, err
	}

	serverStackTrace, err := io_util.ReadUTF(in)
	if err != nil {
		return nil, err
	}

	return NewInvalidEventNumber(*writerId, eventNumber, serverStackTrace), nil
}
