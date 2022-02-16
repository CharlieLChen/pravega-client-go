package protocal

import (
	"io"
	io2 "io.pravega.pravega-client-go/io"
)

type HelloConstructor struct {
}

type Constructor interface {
	ReadFrom(in io.Reader, length int) (WireCommand, error)
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
