package protocol

import (
	"io"
	io_util "io.pravega.pravega-client-go/io"
)

// ==== SetupAppend
type WrongHost struct {
	Type             *WireCommandType
	RequestId        int64
	Segment          string
	CorrectHost      string
	ServerStackTrace string
}

func NewWrongHost(requestId int64, segment, correctHost, serverStackTrace string) *WrongHost {
	return &WrongHost{
		Type:             TypeWrongHost,
		RequestId:        requestId,
		CorrectHost:      correctHost,
		Segment:          segment,
		ServerStackTrace: serverStackTrace,
	}
}
func (wrongHost *WrongHost) GetType() *WireCommandType {
	return TypeWrongHost
}

func (wrongHost *WrongHost) WriteFields(buffer *io_util.ByteBuffer) error {
	err := buffer.WriteInt64(wrongHost.RequestId)
	if err != nil {
		return err
	}
	err = buffer.WriteUTF(wrongHost.Segment)
	if err != nil {
		return err
	}
	err = buffer.WriteUTF(wrongHost.CorrectHost)
	if err != nil {
		return err
	}
	err = buffer.WriteUTF(wrongHost.ServerStackTrace)
	if err != nil {
		return err
	}
	return nil
}

func (wrongHost *WrongHost) GetRequestId() int64 {
	return wrongHost.RequestId
}

func (wrongHost *WrongHost) IsFailure() bool {
	return true
}

type WrongHostConstructor struct {
}

func (constructor *WrongHostConstructor) ReadFrom(in io.Reader, length int32) (WireCommand, error) {
	requestId, err := io_util.ReadInt64(in)
	if err != nil {
		return nil, err
	}
	segment, err := io_util.ReadUTF(in)
	if err != nil {
		return nil, err
	}

	correctHost, err := io_util.ReadUTF(in)
	if err != nil {
		return nil, err
	}
	serverStackTrace, err := io_util.ReadUTF(in)
	if err != nil {
		return nil, err
	}
	return NewWrongHost(requestId, segment, correctHost, serverStackTrace), nil
}
