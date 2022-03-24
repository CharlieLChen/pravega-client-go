package protocal

import (
	bytes2 "bytes"
	"github.com/google/uuid"
	io_util "io.pravega.pravega-client-go/io"
)

type Constructor interface {
	ReadFrom(data []byte, length int32) (WireCommand, error)
}

type HelloConstructor struct {
}

func (constructor HelloConstructor) ReadFrom(data []byte, length int32) (WireCommand, error) {
	in := bytes2.NewBuffer(data)
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

func (constructor AppendSetupConstructor) ReadFrom(data []byte, length int32) (WireCommand, error) {
	in := bytes2.NewBuffer(data)
	var requestId int64
	err := io_util.Read(in, &requestId)
	if err != nil {
		return nil, err
	}
	utf, err := io_util.ReadUTF(in)
	if err != nil {
		return nil, err
	}
	bytes, err := io_util.ReadWithLength(in, 16)
	if err != nil {
		return nil, err
	}
	writerId, err := uuid.FromBytes(bytes)
	if err != nil {
		return nil, err
	}
	var lastEventNumber int64
	err = io_util.Read(in, &requestId)
	if err != nil {
		return nil, err
	}
	return NewAppendSetup(requestId, writerId, utf, lastEventNumber), nil
}
