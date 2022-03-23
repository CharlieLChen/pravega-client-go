package segment

import (
	"fmt"
	"github.com/google/uuid"
	"io.pravega.pravega-client-go/connection"
	v1 "io.pravega.pravega-client-go/controller/proto"
	"io.pravega.pravega-client-go/protocal"
	"io.pravega.pravega-client-go/protocal/event_wrap"
	"io.pravega.pravega-client-go/util"
)

type SegmentStoreHandler struct {
	sockets             connection.Sockets
	encoder             *connection.CommandEncoder
	writeId             uuid.UUID
	segmentId           *v1.SegmentId
	segmentName         string
	requestId           int64
	appendStartPosition int
	eventStartPosition  int
	eventCountPerBatch  int
}

func NewSegmentStoreHandler(segmentId *v1.SegmentId, writeId uuid.UUID, requestId int64, sockets connection.Sockets) *SegmentStoreHandler {
	handler := &SegmentStoreHandler{
		writeId:   writeId,
		requestId: requestId,
		sockets:   sockets,
		segmentId: segmentId,
	}
	handler.segmentName = util.GetQualifiedStreamSegmentName(segmentId)
	handler.encoder = connection.NewCommandEncoder()
	handler.reset()
	return handler
}
func (handler *SegmentStoreHandler) SendCommand(cmd protocal.WireCommand) error {
	handler.encoder.Reset()
	encoded := handler.encoder.EncodeCommand(cmd)
	_, err := handler.sockets.Write(handler.segmentName, encoded.Data())
	if err != nil {
		return err
	}
	handler.encoder.Reset()
	return nil
}

func getBlockSize() int {
	return 0
}
func (handler *SegmentStoreHandler) reset() {
	handler.encoder.Reset()
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
		err = handler.encoder.WriteIntAt(handler.appendStartPosition+connection.TypeSize, int32(bufferedDataSize+overhead-connection.TypePlusLengthSize))
		if err != nil {
			return err
		}
		data := buffer.Data()
		fmt.Printf("%v\n", data)
		_, err := handler.sockets.Write(handler.segmentName, data)
		if err != nil {
			return err
		}
	}

	return nil
}
