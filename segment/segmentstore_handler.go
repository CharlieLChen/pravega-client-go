package segment

import (
	"io.pravega.pravega-client-go/protocol"
	"io.pravega.pravega-client-go/util"
	"time"
)

type AppendHandler struct {
	segmentOutputStream *SegmentOutputStream
	encoder             *protocol.CommandEncoder
	segmentName         string
	appendStartPosition int
	eventStartPosition  int
	eventCountPerBatch  int
	flushTime           time.Time
}

func NewAppendHandler(segmentOutputStream *SegmentOutputStream) *AppendHandler {
	handler := &AppendHandler{
		segmentOutputStream: segmentOutputStream,
	}
	handler.segmentName = util.GetQualifiedStreamSegmentName(segmentOutputStream.segmentId)
	handler.encoder = protocol.NewCommandEncoder()
	handler.reset()
	handler.flushTime = time.Now()
	return handler
}
func (handler *AppendHandler) SendCommand(cmd protocol.WireCommand) (protocol.Reply, error) {
	handler.encoder.Reset()
	encoded := handler.encoder.EncodeCommand(cmd)
	response, err := handler.segmentOutputStream.sockets.Write(handler.segmentName, encoded.Data())
	if err != nil {
		return nil, err
	}
	handler.encoder.Reset()
	return response, nil
}

func getBlockSize() int {
	return 0
}
func (handler *AppendHandler) reset() {
	handler.encoder.Reset()
	handler.eventCountPerBatch = 0
	handler.appendStartPosition = -1
	handler.eventStartPosition = -1
}
func (handler *AppendHandler) startAppend() {
	appendBlock := protocol.NewAppendBlock(handler.segmentOutputStream.writerId)
	handler.appendStartPosition = handler.encoder.Buffer.Buffered()
	handler.encoder.EncodeAppendBlock(appendBlock)
	handler.eventStartPosition = handler.encoder.Buffer.Buffered()
}
func (handler *AppendHandler) appendStarted() bool {
	return handler.appendStartPosition != -1
}
func (handler *AppendHandler) writeData(event []byte) error {
	return handler.encoder.Buffer.Write(event)
}
func (handler *AppendHandler) Buffered() []byte {
	return handler.encoder.Buffer.Data()
}

func (handler *AppendHandler) SendAppend(append *protocol.Append) (bool, error) {
	if !handler.appendStarted() {
		handler.startAppend()
	}
	err := handler.writeData(append.Data)
	if err != nil {
		return false, err
	}

	handler.eventCountPerBatch++
	suggestedBlockSize := getBlockSize()
	overhead := handler.eventStartPosition - handler.appendStartPosition
	bufferedDataSize := handler.encoder.Buffer.Buffered() - overhead
	if (suggestedBlockSize - bufferedDataSize) <= 0 {
		end := protocol.NewAppendBlockEnd(handler.segmentOutputStream.writerId, int32(bufferedDataSize), int32(handler.eventCountPerBatch), append.EventNumber, handler.segmentOutputStream.requestId)
		handler.encoder.EncodeCommand(end)
		err := handler.encoder.WriteIntAt(handler.appendStartPosition+protocol.TypeSize, int32(bufferedDataSize+overhead-protocol.TypePlusLengthSize))
		if err != nil {
			return false, err
		}
		handler.flushTime = time.Now()
		return true, nil
	}
	return false, nil

}

func (handler *AppendHandler) bufferComplete() error {
	if !handler.appendStarted() {
		return nil
	}

	overhead := handler.eventStartPosition - handler.appendStartPosition
	bufferedDataSize := handler.encoder.Buffer.Buffered() - overhead

	end := protocol.NewAppendBlockEnd(handler.segmentOutputStream.writerId, int32(bufferedDataSize), int32(handler.eventCountPerBatch),
		handler.segmentOutputStream.eventNumber, handler.segmentOutputStream.requestId)
	handler.encoder.EncodeCommand(end)
	err := handler.encoder.WriteIntAt(handler.appendStartPosition+protocol.TypeSize, int32(bufferedDataSize+overhead-protocol.TypePlusLengthSize))
	if err != nil {
		return err
	}

	handler.flushTime = time.Now()
	return nil
}
