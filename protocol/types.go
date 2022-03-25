package protocol

import (
	io_util "io.pravega.pravega-client-go/io"
)

const (
	WireVersion             = 15
	OldestCompatibleVersion = 5
)

type WireCommand interface {
	GetType() *WireCommandType
	WriteFields(buffer *io_util.ByteBuffer) error
}

type Reply interface {
	WireCommand
	GetRequestId() int64
	IsFailure() bool
}

type WireCommandType struct {
	Code    int32
	Factory Constructor
}

func NewWireCommandType(code int32, factory Constructor) *WireCommandType {
	return &WireCommandType{
		Code:    code,
		Factory: factory,
	}
}

var (
	TypesMapping = map[int32]*WireCommandType{}
	// Start to define

	TypeHello                  = NewWireCommandType(-127, HelloConstructor{})
	TypeEvent                  = NewWireCommandType(0, nil)
	TypeSetupAppend            = NewWireCommandType(1, nil)
	TypeAppendSetup            = NewWireCommandType(2, AppendSetupConstructor{})
	TypeAppendBlock            = NewWireCommandType(3, nil)
	TypeAppendBlockEnd         = NewWireCommandType(4, nil)
	TypeConditionalAppend      = NewWireCommandType(5, nil)
	TypeDataAppended           = NewWireCommandType(7, nil)
	TypeConditionalCheckFailed = NewWireCommandType(8, nil)
	TypeReadSegment            = NewWireCommandType(9, nil)
	TypeSegmentRead            = NewWireCommandType(10, nil)

	TypeWrongHost            = NewWireCommandType(50, nil)
	TypeSegmentSealed        = NewWireCommandType(51, nil)
	TypeSegmentExists        = NewWireCommandType(52, nil)
	TypeNoSuchSegment        = NewWireCommandType(53, nil)
	TypeInvalidEventNumber   = NewWireCommandType(55, nil)
	TypeSegmentTruncated     = NewWireCommandType(56, nil)
	TypeOperationUnsupported = NewWireCommandType(57, nil)

	TypeKeepAlive = NewWireCommandType(100, nil)
)

func init() {
	TypesMapping[TypeHello.Code] = TypeHello
	TypesMapping[TypeEvent.Code] = TypeEvent
	TypesMapping[TypeSetupAppend.Code] = TypeSetupAppend
	TypesMapping[TypeAppendSetup.Code] = TypeAppendSetup
	TypesMapping[TypeAppendBlock.Code] = TypeAppendBlock
	TypesMapping[TypeAppendBlockEnd.Code] = TypeAppendBlockEnd
	TypesMapping[TypeConditionalAppend.Code] = TypeConditionalAppend
	TypesMapping[TypeDataAppended.Code] = TypeDataAppended
	TypesMapping[TypeConditionalCheckFailed.Code] = TypeConditionalCheckFailed
	TypesMapping[TypeReadSegment.Code] = TypeReadSegment
	TypesMapping[TypeSegmentRead.Code] = TypeSegmentRead

	TypesMapping[TypeWrongHost.Code] = TypeWrongHost
	TypesMapping[TypeSegmentSealed.Code] = TypeSegmentSealed
	TypesMapping[TypeSegmentExists.Code] = TypeSegmentExists
	TypesMapping[TypeNoSuchSegment.Code] = TypeNoSuchSegment
	TypesMapping[TypeInvalidEventNumber.Code] = TypeInvalidEventNumber
	TypesMapping[TypeSegmentTruncated.Code] = TypeSegmentTruncated
	TypesMapping[TypeOperationUnsupported.Code] = TypeOperationUnsupported

	TypesMapping[TypeKeepAlive.Code] = TypeKeepAlive
}
