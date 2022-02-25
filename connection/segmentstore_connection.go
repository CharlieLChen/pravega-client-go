package connection

import (
	"fmt"
	"github.com/google/uuid"
	"io.pravega.pravega-client-go/protocal"
	"io.pravega.pravega-client-go/protocal/event_wrap"
	"net"
)

type SegmentStoreHandler struct {
	address             string
	port                int32
	connection          *net.TCPConn
	encoder             *CommandEncoder
	writeId             uuid.UUID
	requestId           int64
	appendStartPosition int
	eventStartPosition  int
	eventCountPerBatch  int
}

func NewSegmentStoreHandler(address string, port int32, writeId uuid.UUID, requestId int64) (*SegmentStoreHandler, error) {
	handler := &SegmentStoreHandler{
		address:   address,
		port:      port,
		writeId:   writeId,
		requestId: requestId,
	}
	url := fmt.Sprintf("%s:%v", address, port)
	con, err := net.Dial("tcp", url)
	if err != nil {
		return nil, err
	}
	handler.connection = con.(*net.TCPConn)
	handler.encoder = NewCommandEncoder()
	handler.reset()
	return handler, nil
}
func (handler *SegmentStoreHandler) SendCommand(cmd protocal.WireCommand) error {
	handler.encoder.reset()
	encoded := handler.encoder.EncodeCommand(cmd)
	_, err := handler.connection.Write(encoded.Data())
	if err != nil {
		return err
	}
	handler.encoder.reset()
	return nil
}

func getBlockSize() int {
	return 0
}
func (handler *SegmentStoreHandler) reset() {
	handler.encoder.reset()
	handler.eventCountPerBatch = 0
	handler.appendStartPosition = -1
	handler.eventStartPosition = -1
}
func (handler *SegmentStoreHandler) startAppend() {
	appendBlock := protocal.NewAppendBlock(handler.writeId)
	handler.appendStartPosition = handler.encoder.Buffer.Buffered()
	handler.encoder.EncodeAppendBlock(appendBlock)
	handler.eventStartPosition = handler.encoder.Buffer.Buffered()
}
func (handler *SegmentStoreHandler) appendStarted() bool {
	return handler.appendStartPosition != -1
}

func (handler *SegmentStoreHandler) SendAppend(append *event_wrap.Append) error {
	if !handler.appendStarted() {
		handler.startAppend()
	}
	err := handler.encoder.EncodeEvent(append.Data)
	handler.eventCountPerBatch++
	if err != nil {
		return err
	}
	suggestedBlockSize := getBlockSize()
	overhead := handler.eventStartPosition - handler.appendStartPosition
	bufferedDataSize := handler.encoder.Buffer.Buffered() - overhead

	if (suggestedBlockSize - bufferedDataSize) <= 0 {
		end := protocal.NewAppendBlockEnd(handler.writeId, int32(bufferedDataSize), int32(handler.eventCountPerBatch), append.EventNumber, handler.requestId)
		fmt.Printf("writerId: %v", end.WriterId.String())
		buffer := handler.encoder.EncodeCommand(end)
		err = handler.encoder.WriteIntAt(handler.appendStartPosition+TypeSize, int32(bufferedDataSize+overhead-TypePlusLengthSize))
		if err != nil {
			return err
		}
		data := buffer.Data()
		fmt.Printf("%v\n", data)
		_, err := handler.connection.Write(data)
		if err != nil {
			return err
		}
	}

	return nil
}
