package protocol

import (
	"io"
	io_util "io.pravega.pravega-client-go/io"
)

type Constructor interface {
	ReadFrom(reader io.Reader, length int32) (WireCommand, error)
}

type HelloConstructor struct {
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

type AppendSetupConstructor struct {
}

func (appendSetup *AppendSetup) WriteFields(buffer *io_util.ByteBuffer) error {

	err := buffer.WriteInt64(appendSetup.RequestId)
	if err != nil {
		return err
	}
	err = buffer.WriteUTF(appendSetup.Segment)
	if err != nil {
		return err
	}
	err = buffer.WriteUUid(appendSetup.WriterId)
	if err != nil {
		return err
	}
	err = buffer.WriteInt64(appendSetup.LastEventNumber)
	if err != nil {
		return err
	}
	return nil
}

func (constructor AppendSetupConstructor) ReadFrom(in io.Reader, length int32) (WireCommand, error) {
	requestId, err := io_util.ReadInt64(in)
	if err != nil {
		return nil, err
	}
	utf, err := io_util.ReadUTF(in)
	if err != nil {
		return nil, err
	}
	writerId, err := io_util.ReadUUid(in)
	if err != nil {
		return nil, err
	}
	lastEventNumber, err := io_util.ReadInt64(in)
	if err != nil {
		return nil, err
	}
	return NewAppendSetup(requestId, *writerId, utf, lastEventNumber), nil
}
