package protocal

import "io"

type WireCommand interface {
	GetType() *WireCommandType
	WriteFields(out io.Writer) error
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
	WirecommandtypeHello = NewWireCommandType(-127, HelloConstructor{})
	WirecommandtypeEvent = NewWireCommandType(0, nil)
)
