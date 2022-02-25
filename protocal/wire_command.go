package protocal

import (
	"bytes"
	"github.com/google/uuid"
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

func NewEvent(data []byte) *Event {
	return &Event{
		Type: WirecommandtypeEvent,
		Data: data,
	}
}

func (event *Event) GetType() *WireCommandType {
	return WirecommandtypeEvent
}

func (event *Event) WriteFields(out io.Writer) error {
	err := io2.Write(out, WirecommandtypeEvent.Code)
	if err != nil {
		return err
	}
	length := len(event.Data)
	err = io2.Write(out, int32(length))
	if err != nil {
		return err
	}
	_, err = out.Write(event.Data)
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
		Type:            WirecommandtypeSetupAppend,
		RequestId:       requestId,
		WriterId:        writerId,
		Segment:         segment,
		DelegationToken: delegationToken,
	}
}
func (event *SetupAppend) GetType() *WireCommandType {
	return WirecommandtypeSetupAppend
}

func (setup *SetupAppend) WriteFields(out io.Writer) error {
	err := io2.Write(out, setup.RequestId)
	if err != nil {
		return err
	}
	binary, err := setup.WriterId.MarshalBinary()
	if err != nil {
		return err
	}
	err = io2.Write(out, binary)
	if err != nil {
		return err
	}
	err = io2.WriteUTF(out, setup.Segment)
	if err != nil {
		return err
	}
	err = io2.WriteUTF(out, setup.DelegationToken)
	if err != nil {
		return err
	}
	return nil
}

// ==== APPEND_SETUP
type AppendSetup struct {
	Type            *WireCommandType
	RequestId       int64
	WriterId        uuid.UUID
	Segment         string
	LastEventNumber int64
}

func NewAppendSetup(requestId int64, writerId uuid.UUID, segment string, lastEventNumber int64) *AppendSetup {
	return &AppendSetup{
		Type:            WirecommandtypeAppendSetup,
		RequestId:       requestId,
		WriterId:        writerId,
		Segment:         segment,
		LastEventNumber: lastEventNumber,
	}
}
func (appendSetup *AppendSetup) GetType() *WireCommandType {
	return WirecommandtypeAppendSetup
}

func (appendSetup *AppendSetup) WriteFields(out io.Writer) error {
	// No need for client for now
	return nil
}

// ==== APPEND_BLOCK
type AppendBlock struct {
	Type     *WireCommandType
	WriterId uuid.UUID
}

func NewAppendBlock(writerId uuid.UUID) *AppendBlock {
	return &AppendBlock{
		WriterId: writerId,
	}
}
func (appendBlock *AppendBlock) GetType() *WireCommandType {
	return WirecommandtypeAppendBlock
}

func (appendBlock *AppendBlock) WriteFields(out io.Writer) error {
	binary, err := appendBlock.WriterId.MarshalBinary()
	if err != nil {
		return err
	}
	_, err = out.Write(binary)
	if err != nil {
		return err
	}
	return nil
}

type AppendBlockEnd struct {
	Type              *WireCommandType
	WriterId          uuid.UUID
	SizeOfWholeEvents int32
	PartialData       []byte
	NumEvents         int32
	LastEventNumber   int64
	RequestId         int64
}

func NewAppendBlockEnd(writerId uuid.UUID, sizeOfWholeEvents int32, numEvents int32, lastEventNumber int64, requestId int64) *AppendBlockEnd {
	return &AppendBlockEnd{
		WriterId:          writerId,
		SizeOfWholeEvents: sizeOfWholeEvents,
		NumEvents:         numEvents,
		LastEventNumber:   lastEventNumber,
		RequestId:         requestId,
	}
}

func (appendBlockEnd *AppendBlockEnd) GetType() *WireCommandType {
	return WirecommandtypeAppendBlockEnd
}

func (appendBlockEnd *AppendBlockEnd) WriteFields(out io.Writer) error {
	binary, err := appendBlockEnd.WriterId.MarshalBinary()
	if err != nil {
		return err
	}
	_, err = out.Write(binary)
	if err != nil {
		return err
	}
	err = io2.Write(out, appendBlockEnd.SizeOfWholeEvents)
	if err != nil {
		return err
	}
	err = io2.Write(out, int32(0)) //never has partial data
	if err != nil {
		return err
	}
	err = io2.Write(out, appendBlockEnd.NumEvents)
	if err != nil {
		return err
	}
	err = io2.Write(out, appendBlockEnd.LastEventNumber)
	if err != nil {
		return err
	}
	err = io2.Write(out, appendBlockEnd.RequestId)
	if err != nil {
		return err
	}
	return nil
}
