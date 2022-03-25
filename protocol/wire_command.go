package protocol

import (
	"github.com/google/uuid"
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

// ==== EVENT
type Event struct {
	Type *WireCommandType
	Data []byte
}

func NewEvent(data []byte) *Event {
	return &Event{
		Type: TypeEvent,
		Data: data,
	}
}

func (event *Event) GetType() *WireCommandType {
	return TypeEvent
}

func (event *Event) WriteFields(buffer *io_util.ByteBuffer) error {
	err := buffer.WriteInt32(TypeEvent.Code)
	if err != nil {
		return err
	}
	length := len(event.Data)
	err = buffer.WriteInt32(int32(length))
	if err != nil {
		return err
	}
	err = buffer.Write(event.Data)
	if err != nil {
		return err
	}
	return nil
}

func (event *Event) GetEncodedData() ([]byte, error) {
	buffer := io_util.NewByteBuffer(0)
	err := event.WriteFields(buffer)
	if err != nil {
		return nil, err
	}
	return buffer.Data(), nil
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
		Type:            TypeSetupAppend,
		RequestId:       requestId,
		WriterId:        writerId,
		Segment:         segment,
		DelegationToken: delegationToken,
	}
}
func (event *SetupAppend) GetType() *WireCommandType {
	return TypeSetupAppend
}

func (setup *SetupAppend) WriteFields(buffer *io_util.ByteBuffer) error {
	err := buffer.WriteInt64(setup.RequestId)
	if err != nil {
		return err
	}
	binary, err := setup.WriterId.MarshalBinary()
	if err != nil {
		return err
	}
	err = buffer.Write(binary)
	if err != nil {
		return err
	}
	err = buffer.WriteUTF(setup.Segment)
	if err != nil {
		return err
	}
	err = buffer.WriteUTF(setup.DelegationToken)
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
		Type:            TypeAppendSetup,
		RequestId:       requestId,
		WriterId:        writerId,
		Segment:         segment,
		LastEventNumber: lastEventNumber,
	}
}
func (appendSetup *AppendSetup) GetType() *WireCommandType {
	return TypeAppendSetup
}

func (appendSetup *AppendSetup) GetRequestId() int64 {
	return appendSetup.RequestId
}

func (appendSetup *AppendSetup) IsFailure() bool {
	return false
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
	return TypeAppendBlock
}

func (appendBlock *AppendBlock) WriteFields(buffer *io_util.ByteBuffer) error {
	binary, err := appendBlock.WriterId.MarshalBinary()
	if err != nil {
		return err
	}
	err = buffer.Write(binary)
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
	return TypeAppendBlockEnd
}

func (appendBlockEnd *AppendBlockEnd) WriteFields(buffer *io_util.ByteBuffer) error {
	err := buffer.WriteUUid(appendBlockEnd.WriterId)
	if err != nil {
		return err
	}
	err = buffer.WriteInt32(appendBlockEnd.SizeOfWholeEvents)
	if err != nil {
		return err
	}
	err = buffer.WriteInt32(int32(0)) //never has partial data
	if err != nil {
		return err
	}
	err = buffer.WriteInt32(appendBlockEnd.NumEvents)
	if err != nil {
		return err
	}
	err = buffer.WriteInt64(appendBlockEnd.LastEventNumber)
	if err != nil {
		return err
	}
	err = buffer.WriteInt64(appendBlockEnd.RequestId)
	if err != nil {
		return err
	}
	return nil
}
