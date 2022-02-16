package protocal

import (
	"bytes"
	"io"
	io2 "io.pravega.pravega-client-go/io"
)

type Hello struct {
	Type        *WireCommandType
	HighVersion int32
	LowVersion  int32
}

func NewHello(highVersion, lowVersion int32) *Hello {
	return &Hello{
		Type:        WirecommandtypeHello,
		HighVersion: highVersion,
		LowVersion:  lowVersion,
	}
}

func (hello *Hello) GetType() *WireCommandType {
	return WirecommandtypeHello
}

func (hello *Hello) WriteFields(out io.Writer) error {
	err := io2.Write(out, hello.HighVersion)
	if err != nil {
		return err
	}
	err = io2.Write(out, hello.LowVersion)
	if err != nil {
		return err
	}
	return nil
}

// ==== EVENT
type Event struct {
	Type *WireCommandType
	Data []byte
}

func (event *Event) GetType() *WireCommandType {
	return WirecommandtypeEvent
}

func (event *Event) WriteFields(out io.Writer) error {
	err := io2.Write(out, WirecommandtypeEvent.Code)
	if err != nil {
		return err
	}
	err = io2.Write(out, len(event.Data))
	if err != nil {
		return err
	}
	err = io2.Write(out, event.Data)
	if err != nil {
		return err
	}
	return nil
}

func (event *Event) GetEncodedData() ([]byte, error) {
	buffer := &bytes.Buffer{}
	err := event.WriteFields(buffer)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}
