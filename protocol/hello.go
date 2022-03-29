package protocol

import (
	"io"
	io_util "io.pravega.pravega-client-go/io"
)

type Hello struct {
	Type        *WireCommandType
	HighVersion int32
	LowVersion  int32
}

func NewHello(highVersion, lowVersion int32) *Hello {
	return &Hello{
		Type:        TypeHello,
		HighVersion: highVersion,
		LowVersion:  lowVersion,
	}
}

func (hello *Hello) GetType() *WireCommandType {
	return TypeHello
}

func (hello *Hello) GetRequestId() int64 {
	return 0
}

func (hello *Hello) WriteFields(buffer *io_util.ByteBuffer) error {
	err := buffer.WriteInt32(hello.HighVersion)
	if err != nil {
		return err
	}
	err = buffer.WriteInt32(hello.LowVersion)
	if err != nil {
		return err
	}
	return nil
}

type HelloConstructor struct {
}

func (constructor HelloConstructor) ReadFrom(in io.Reader, length int32) (WireCommand, error) {
	var highVersion int32
	var lowerVersion int32
	err := io_util.Read(in, &highVersion)
	if err != nil {
		return nil, err
	}
	err = io_util.Read(in, &lowerVersion)
	if err != nil {
		return nil, err
	}
	hello := NewHello(highVersion, lowerVersion)
	return hello, nil
}
