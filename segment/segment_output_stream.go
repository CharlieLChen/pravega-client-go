package segment

import (
	"fmt"
	"github.com/google/uuid"
	"io.pravega.pravega-client-go/connection"
	"io.pravega.pravega-client-go/controller"
	v1 "io.pravega.pravega-client-go/controller/proto"
	"io.pravega.pravega-client-go/errors"
	"io.pravega.pravega-client-go/protocal"
	"io.pravega.pravega-client-go/protocal/event_wrap"
	"io.pravega.pravega-client-go/security/auth"
	"io.pravega.pravega-client-go/util"
	"log"
	"sync/atomic"
)

type SegmentOutputStream struct {
	segmentId      *v1.SegmentId
	controller     *controller.Controller
	writerId       uuid.UUID
	tokenProvider  *auth.EmptyDelegationTokenProvider
	requestId      int64
	sockets        connection.Sockets
	eventNumber    int64
	handler        *SegmentStoreHandler
	setupCompleted int32
	response       chan protocal.Reply
	responseClient *connection.ResponseClient
	dispatcher     connection.ResponseDispatcher
}

var (
	SetupUnCompleted = int32(0)
	SetupCompleted   = int32(1)
)

func NewSegmentOutputStream(segmentId *v1.SegmentId, controller *controller.Controller, sockets connection.Sockets, dispatcher connection.ResponseDispatcher) *SegmentOutputStream {
	writerId, _ := uuid.NewUUID()
	fmt.Printf("writerId: %v, segmentId:%v", writerId.String(), segmentId.String())
	tokenProvider := &auth.EmptyDelegationTokenProvider{}
	requestId := connection.NewFlow().AsLong()
	response := make(chan protocal.Reply, 10)
	dispatcher.RegisterClient(requestId, response)

	segmentOutput := &SegmentOutputStream{
		segmentId:      segmentId,
		controller:     controller,
		writerId:       writerId,
		tokenProvider:  tokenProvider,
		requestId:      requestId,
		eventNumber:    0,
		sockets:        sockets,
		setupCompleted: SetupUnCompleted,
		response:       response,
	}
	segmentStoreHandler := NewSegmentStoreHandler(segmentOutput.segmentId, segmentOutput.writerId, segmentOutput.requestId, segmentOutput.sockets)
	segmentOutput.handler = segmentStoreHandler
	segmentOutput.responseClient = connection.NewResponseClient()
	go segmentOutput.handleResponse()
	return segmentOutput
}

func (segmentOutput *SegmentOutputStream) Write(data []byte) error {
	event := &protocal.Event{
		Data: data,
	}
	encodedData, err := event.GetEncodedData()
	if err != nil {
		return err
	}
	segmentOutput.eventNumber = segmentOutput.eventNumber + 1
	append := event_wrap.NewAppend(segmentOutput.segmentId, segmentOutput.writerId, segmentOutput.eventNumber, encodedData, segmentOutput.requestId)
	return segmentOutput.send(append)

}
func (segmentOutput *SegmentOutputStream) setupAppend() error {
	segmentName := util.GetQualifiedStreamSegmentName(segmentOutput.segmentId)
	token := segmentOutput.tokenProvider.RetrieveToken()
	setupAppend := protocal.NewSetupAppend(segmentOutput.requestId, segmentOutput.writerId, segmentName, token)
	for {
		err := segmentOutput.handler.SendCommand(setupAppend)
		if err != nil {
			return err
		}
		res, err := segmentOutput.responseClient.GetAppendSetup(connection.WaitEndless)
		if err != nil {
			if err == errors.Error_Timeout {
				return err
			}
			if res.IsFailure() {
				retry, err1 := segmentOutput.handleFailure(res)
				if err1 != nil {
					return fmt.Errorf("can't to handle failure for %v", res.GetType())
				}
				if !retry {
					return fmt.Errorf("abort to set up due to %v", res.GetType())
				}
				log.Printf("hitting errror: %v, retrying to setup", res.GetType())
			}
		}
		//success to handle
		return nil
	}

	return nil
}
func (segmentOutput *SegmentOutputStream) send(append *event_wrap.Append) error {
	if atomic.LoadInt32(&segmentOutput.setupCompleted) == SetupUnCompleted {
		err := segmentOutput.setupAppend()
		if err != nil {
			return err
		}
	}

	err := segmentOutput.handler.SendAppend(append)
	if err != nil {
		return err
	}
	return nil
}

func (segmentOutput *SegmentOutputStream) handleFailure(response protocal.Reply) (bool, error) {
	if !response.IsFailure() {
		return false, nil
	}
	return true, nil
}

func (segmentOutput *SegmentOutputStream) handleResponse() {
	for response := range segmentOutput.response {
		segmentOutput.responseClient.Offer(response)
	}
}
