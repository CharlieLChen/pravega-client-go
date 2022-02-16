package segment

import (
	"github.com/google/uuid"
	"io.pravega.pravega-client-go/connection"
	"io.pravega.pravega-client-go/controller-client"
	"io.pravega.pravega-client-go/protocal"
	"io.pravega.pravega-client-go/protocal/event_wrap"
	"io.pravega.pravega-client-go/security/auth"
)

type SegmentOutputStream struct {
	segmentName   string
	controller    controller.Controller
	writerId      uuid.UUID
	tokenProvider *auth.EmptyDelegationTokenProvider
	requestId     int64
	state         *SegmentOutputStreamState
}

type SegmentOutputStreamState struct {
	EventNumber int64
	connection  *connection.SegmentStoreConnection
}

func NewSegmentOutputStream(segmentName string, controller controller.Controller) *SegmentOutputStream {
	writerId, _ := uuid.NewUUID()
	tokenProvider := &auth.EmptyDelegationTokenProvider{}
	requestId := connection.NewFlow().AsLong()
	return &SegmentOutputStream{
		segmentName:   segmentName,
		controller:    controller,
		writerId:      writerId,
		tokenProvider: tokenProvider,
		requestId:     requestId,
		state:         &SegmentOutputStreamState{EventNumber: 0},
	}
}

func (segmentOutput *SegmentOutputStream) write(data []byte) error {
	event := &protocal.Event{
		Data: data,
	}
	encodedData, err := event.GetEncodedData()
	if err != nil {
		return nil
	}
	segmentOutput.state.EventNumber = segmentOutput.state.EventNumber + 1
	append := event_wrap.NewAppend(segmentOutput.segmentName, segmentOutput.writerId, segmentOutput.state.EventNumber, encodedData, segmentOutput.requestId)

	return nil
}

func (segmentOutput *SegmentOutputStream) send(append *event_wrap.Append) {
	if segmentOutput.state.connection == nil {

	}
}
