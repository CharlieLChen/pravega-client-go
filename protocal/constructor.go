package protocal

import (
	"github.com/google/uuid"
	"io"
	io2 "io.pravega.pravega-client-go/io"
)

type Constructor interface {
	ReadFrom(in io.Reader, length int) (WireCommand, error)
}

type HelloConstructor struct {
}

func (constructor HelloConstructor) ReadFrom(in io.Reader, length int) (WireCommand, error) {
	var highVersion int32
	var lowerVersion int32
	err := io2.Read(in, &highVersion)
	if err != nil {
		return nil, err
	}
	err = io2.Read(in, &lowerVersion)
	if err != nil {
		return nil, err
	}
	hello := NewHello(highVersion, lowerVersion)
	return hello, nil
}

type AppendSetupConstructor struct {
}

func (constructor AppendSetupConstructor) ReadFrom(in io.Reader, length int) (WireCommand, error) {
	var requestId int64
	err := io2.Read(in, &requestId)
	if err != nil {
		return nil, err
	}
	utf, err := io2.ReadUTF(in)
	if err != nil {
		return nil, err
	}
	bytes, err := io2.ReadWithLength(in, 16)
	if err != nil {
		return nil, err
	}
	writerId, err := uuid.FromBytes(bytes)
	if err != nil {
		return nil, err
	}
	var lastEventNumber int64
	err = io2.Read(in, &requestId)
	if err != nil {
		return nil, err
	}
	return NewAppendSetup(requestId, writerId, utf, lastEventNumber), nil
}
