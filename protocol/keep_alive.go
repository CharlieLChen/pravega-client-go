package protocol

import (
	"io"
	io_util "io.pravega.pravega-client-go/io"
)

type KeepAlive struct {
	Type *WireCommandType
}

func NewKeepAlive() *KeepAlive {
	return &KeepAlive{
		Type: TypeKeepAlive,
	}
}

func (keepAlive *KeepAlive) GetType() *WireCommandType {
	return TypeKeepAlive
}

func (keepAlive *KeepAlive) IsFailure() bool {
	return false
}

func (keepAlive *KeepAlive) GetRequestId() int64 {
	return -1
}

func (keepAlive *KeepAlive) WriteFields(buffer *io_util.ByteBuffer) error {
	return nil
}

type KeepAliveConstructor struct {
}

func (constructor KeepAliveConstructor) ReadFrom(in io.Reader, length int32) (WireCommand, error) {
	keepAlive := NewKeepAlive()
	return keepAlive, nil
}
