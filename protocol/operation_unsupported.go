package protocol

import (
	"io"
	io_util "io.pravega.pravega-client-go/io"
)

// ==== OperationUnsupported
type OperationUnsupported struct {
	Type             *WireCommandType
	RequestId        int64
	OperationName    string
	ServerStackTrace string
}

func NewOperationUnsupported(requestId int64, operationName, serverStackTrace string) *OperationUnsupported {
	return &OperationUnsupported{
		Type:             TypeOperationUnsupported,
		RequestId:        requestId,
		OperationName:    operationName,
		ServerStackTrace: serverStackTrace,
	}
}
func (operationUnsupported *OperationUnsupported) GetType() *WireCommandType {
	return TypeOperationUnsupported
}

func (operationUnsupported *OperationUnsupported) WriteFields(buffer *io_util.ByteBuffer) error {
	err := buffer.WriteInt64(operationUnsupported.RequestId)
	if err != nil {
		return err
	}
	err = buffer.WriteUTF(operationUnsupported.OperationName)
	if err != nil {
		return err
	}
	err = buffer.WriteUTF(operationUnsupported.ServerStackTrace)
	if err != nil {
		return err
	}
	return nil
}

func (operationUnsupported *OperationUnsupported) GetRequestId() int64 {
	return operationUnsupported.RequestId
}

func (operationUnsupported *OperationUnsupported) IsFailure() bool {
	return true
}

type OperationUnsupportedConstructor struct {
}

func (constructor *OperationUnsupportedConstructor) ReadFrom(in io.Reader, length int32) (WireCommand, error) {
	requestId, err := io_util.ReadInt64(in)
	if err != nil {
		return nil, err
	}
	operationName, err := io_util.ReadUTF(in)
	if err != nil {
		return nil, err
	}

	serverStackTrace, err := io_util.ReadUTF(in)
	if err != nil {
		return nil, err
	}
	return NewOperationUnsupported(requestId, operationName, serverStackTrace), nil
}
