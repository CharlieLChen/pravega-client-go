package segment

import (
	"github.com/google/uuid"
	"io.pravega.pravega-client-go/connection"
	"io.pravega.pravega-client-go/controller"
	v1 "io.pravega.pravega-client-go/controller/proto"
	"io.pravega.pravega-client-go/protocal"
	"io.pravega.pravega-client-go/protocal/event_wrap"
	"io.pravega.pravega-client-go/security/auth"
	"io.pravega.pravega-client-go/util"
)

type SegmentOutputStream struct {
	segmentId     *v1.SegmentId
	controller    controller.Controller
	writerId      uuid.UUID
	tokenProvider *auth.EmptyDelegationTokenProvider
	requestId     int64
	state         *SegmentOutputStreamState
}

type SegmentOutputStreamState struct {
	EventNumber int64
	connection  *connection.SegmentStoreHandler
}

func NewSegmentOutputStream(segmentId *v1.SegmentId, controller controller.Controller) *SegmentOutputStream {
	writerId, _ := uuid.NewUUID()
	tokenProvider := &auth.EmptyDelegationTokenProvider{}
	requestId := connection.NewFlow().AsLong()
	return &SegmentOutputStream{
		segmentId:     segmentId,
		controller:    controller,
		writerId:      writerId,
		tokenProvider: tokenProvider,
		requestId:     requestId,
		state:         &SegmentOutputStreamState{EventNumber: 0},
	}
}

func (segmentOutput *SegmentOutputStream) Write(data []byte) error {
	event := &protocal.Event{
		Data: data,
	}
	encodedData, err := event.GetEncodedData()
	if err != nil {
		return nil
	}
	segmentOutput.state.EventNumber = segmentOutput.state.EventNumber + 1
	append := event_wrap.NewAppend(segmentOutput.segmentId, segmentOutput.writerId, segmentOutput.state.EventNumber, encodedData, segmentOutput.requestId)
	return segmentOutput.send(append)

}
func (segmentOutput *SegmentOutputStream) setupAppend() error {
	segmentName := util.GetQualifiedStreamSegmentName(segmentOutput.segmentId)
	token := segmentOutput.tokenProvider.RetrieveToken()
	setupAppend := protocal.NewSetupAppend(segmentOutput.requestId, segmentOutput.writerId, segmentName, token)
	err := segmentOutput.state.connection.SendCommand(setupAppend)
	if err != nil {
		return err
	}
	return nil
}
func (segmentOutput *SegmentOutputStream) send(append *event_wrap.Append) error {
	if segmentOutput.state.connection == nil {
		uri, err := segmentOutput.controller.GetSegmentStoreURI(segmentOutput.segmentId)
		if err != nil {
			return err
		}
		storeConnection, err := connection.NewSegmentStoreHandler(uri.Endpoint, uri.Port)
		if err != nil {
			return err
		}
		segmentOutput.state.connection = storeConnection
		segmentOutput.setupAppend()
	}
	err := segmentOutput.state.connection.SendAppend(append)
	if err != nil {
		return err
	}
	return nil
}
