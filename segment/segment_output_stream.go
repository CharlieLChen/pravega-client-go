package segment

import (
	"fmt"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"io.pravega.pravega-client-go/connection"
	"io.pravega.pravega-client-go/controller"
	v1 "io.pravega.pravega-client-go/controller/proto"
	"io.pravega.pravega-client-go/errors"
	"io.pravega.pravega-client-go/protocol"
	"io.pravega.pravega-client-go/security/auth"
	"io.pravega.pravega-client-go/util"
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
	response       chan protocol.Reply
	responseClient *connection.ResponseClient
	dispatcher     connection.ResponseDispatcher
	inflight       *util.FIFOList
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
	responseCh := make(chan protocol.Reply, 10)
	dispatcher.RegisterClient(requestId, responseCh)

	segmentOutput := &SegmentOutputStream{
		segmentId:      segmentId,
		controller:     controller,
		writerId:       writerId,
		tokenProvider:  tokenProvider,
		requestId:      requestId,
		eventNumber:    0,
		sockets:        sockets,
		setupCompleted: SetupUnCompleted,
		response:       responseCh,
		inflight:       &util.FIFOList{},
	}
	segmentStoreHandler := NewSegmentStoreHandler(segmentOutput.segmentId, segmentOutput.writerId, segmentOutput.requestId, segmentOutput.sockets)
	segmentOutput.handler = segmentStoreHandler
	segmentOutput.responseClient = connection.NewResponseClient()
	go segmentOutput.receiveResponse()
	return segmentOutput
}

func (segmentOutput *SegmentOutputStream) Write(data []byte) error {
	if atomic.LoadInt32(&segmentOutput.setupCompleted) == SetupUnCompleted {
		err := segmentOutput.setupAppend()
		if err != nil {
			return err
		}
	}
	event := &protocol.Event{
		Data: data,
	}

	encodedData, err := event.GetEncodedData()
	if err != nil {
		return err
	}

	segmentOutput.eventNumber = segmentOutput.eventNumber + 1
	append := protocol.NewAppend(segmentOutput.segmentId, segmentOutput.writerId, segmentOutput.eventNumber, encodedData, segmentOutput.requestId)
	segmentOutput.inflight.Insert(append)

	return segmentOutput.send(append)

}
func (segmentOutput *SegmentOutputStream) setupAppend() error {
	segmentName := util.GetQualifiedStreamSegmentName(segmentOutput.segmentId)
	token := segmentOutput.tokenProvider.RetrieveToken()
	setupAppend := protocol.NewSetupAppend(segmentOutput.requestId, segmentOutput.writerId, segmentName, token)
	for {
		err := segmentOutput.handler.SendCommand(setupAppend)
		if err != nil {
			return err
		}
		res, err := segmentOutput.responseClient.GetAppendSetup(connection.WaitEndless)
		if err != nil {
			// never hit this error if wait forever
			if err == errors.Error_Timeout {
				return err
			}

			if res.IsFailure() {
				retry, err1 := segmentOutput.handleFailure(res)
				if err1 != nil {
					return fmt.Errorf("can't to handle failure for %v", res)
				}
				if !retry {
					return fmt.Errorf("abort to set up due to %v", res)
				}
				log.Errorf("hitting error: %v, retrying to setup", res)
			}
		}
		setup := res.(*protocol.AppendSetup)
		return segmentOutput.handleAppendSetup(setup)
	}

}

func (segmentOutput *SegmentOutputStream) handleAppendSetup(appendSetup *protocol.AppendSetup) error {
	log.Info("Received appendSetup {}", appendSetup)
	ackLevel := appendSetup.LastEventNumber
	segmentOutput.inflight.DeleteTo(canRemove, ackLevel)
	segmentOutput.handler.reset()
	return segmentOutput.inflight.ForEach(segmentOutput, resend)
}
func resend(handler interface{}, a interface{}) error {
	append := a.(*protocol.Append)
	segmentOutput := handler.(*SegmentOutputStream)
	return segmentOutput.send(append)
}

func canRemove(a interface{}, b interface{}) bool {
	append := a.(*protocol.Append)
	number := b.(int64)
	return append.EventNumber <= number

}

func (segmentOutput *SegmentOutputStream) send(append *protocol.Append) error {

	err := segmentOutput.handler.SendAppend(append)
	if err != nil {
		return err
	}
	return nil
}

func (segmentOutput *SegmentOutputStream) handleFailure(response protocol.Reply) (bool, error) {

	return true, nil
}

func (segmentOutput *SegmentOutputStream) receiveResponse() {
	for response := range segmentOutput.response {
		segmentOutput.responseClient.Offer(response)
	}
}
