package segment

import (
	"fmt"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"io.pravega.pravega-client-go/command"
	"io.pravega.pravega-client-go/connection"
	"io.pravega.pravega-client-go/controller"
	v1 "io.pravega.pravega-client-go/controller/proto"
	"io.pravega.pravega-client-go/errors"
	"io.pravega.pravega-client-go/protocol"
	"io.pravega.pravega-client-go/security/auth"
	"io.pravega.pravega-client-go/util"
	"time"
)

type SegmentOutputStream struct {
	segmentId      *v1.SegmentId
	SegmentName    string
	controller     *controller.ControllerImpl
	writerId       uuid.UUID
	tokenProvider  *auth.EmptyDelegationTokenProvider
	requestId      int64
	sockets        *connection.Sockets
	eventNumber    int64
	handler        *AppendHandler
	setupStatus    int32
	response       chan protocol.Reply
	WriteCh        chan []byte
	responseClient *connection.ResponseClient
	dispatcher     *connection.ResponseDispatcher
	inflight       *util.FIFOList
	State          int
	ControlCh      chan *command.Command
}

var (
	SetupUnCompleted = int32(0)
	SetupCompleted   = int32(1)
	Running          = 0
	Stop             = 1
)

func NewSegmentOutputStream(segmentId *v1.SegmentId, controller *controller.ControllerImpl, control chan *command.Command, sockets *connection.Sockets) *SegmentOutputStream {
	writerId, _ := uuid.NewUUID()
	fmt.Printf("writerId: %v, segmentId:%v", writerId.String(), segmentId.String())
	tokenProvider := &auth.EmptyDelegationTokenProvider{}
	requestId := connection.NewFlow().AsLong()
	responseCh := make(chan protocol.Reply, 10)

	segmentOutput := &SegmentOutputStream{
		segmentId:     segmentId,
		controller:    controller,
		writerId:      writerId,
		tokenProvider: tokenProvider,
		requestId:     requestId,
		eventNumber:   0,
		sockets:       sockets,
		ControlCh:     control,
		setupStatus:   SetupUnCompleted,
		response:      responseCh,
		inflight:      &util.FIFOList{},
		SegmentName:   util.GetQualifiedStreamSegmentName(segmentId),
		WriteCh:       make(chan []byte),
	}
	segmentStoreHandler := NewAppendHandler(segmentOutput)
	segmentOutput.handler = segmentStoreHandler
	segmentOutput.responseClient = connection.NewResponseClient()
	go segmentOutput.start()
	go segmentOutput.receiveResponse()
	return segmentOutput
}

func (segmentOutput *SegmentOutputStream) write(data []byte) (bool, error) {
	if segmentOutput.setupStatus == SetupUnCompleted {
		stop, err := segmentOutput.setupAppend()
		if err != nil {
			return stop, err
		}
		if stop {
			return true, nil
		}
	}
	event := &protocol.Event{
		Data: data,
	}

	encodedData, err := event.GetEncodedData()
	if err != nil {
		return false, err
	}

	segmentOutput.eventNumber = segmentOutput.eventNumber + 1
	append := protocol.NewAppend(segmentOutput.segmentId, segmentOutput.writerId, segmentOutput.eventNumber, encodedData, segmentOutput.requestId)
	segmentOutput.inflight.Insert(append)

	return false, segmentOutput.send(append)

}
func (segmentOutput *SegmentOutputStream) setupAppend() (bool, error) {
	segmentName := util.GetQualifiedStreamSegmentName(segmentOutput.segmentId)
	token := segmentOutput.tokenProvider.RetrieveToken()
	setupAppend := protocol.NewSetupAppend(segmentOutput.requestId, segmentOutput.writerId, segmentName, token)
	for {
		err := segmentOutput.handler.SendCommand(setupAppend)
		if err != nil {
			return true, err
		}
		res, err := segmentOutput.responseClient.GetAppendSetup(connection.Forever)
		if err != nil {
			// never hit this error as wait forever
			if err == errors.Error_Timeout {
				return true, err
			}

			if res.IsFailure() {
				stop := segmentOutput.handleFailureResponse(res)
				return stop, nil
			}
		}
		setup := res.(*protocol.AppendSetup)
		return false, segmentOutput.handleAppendSetup(setup)
	}

}

func (segmentOutput *SegmentOutputStream) handleAppendSetup(appendSetup *protocol.AppendSetup) error {
	log.Info("Received appendSetup {}", appendSetup)
	ackLevel := appendSetup.LastEventNumber
	segmentOutput.inflight.DeleteTo(canRemove, ackLevel)
	segmentOutput.handler.reset()

	err := segmentOutput.inflight.ForEach(segmentOutput, resend)
	if err != nil {
		return err
	}
	segmentOutput.setupStatus = SetupCompleted
	return nil
}
func resend(handler interface{}, a interface{}) error {
	append := a.(*protocol.Append)
	segmentOutput := handler.(*SegmentOutputStream)
	append.FlowId = segmentOutput.requestId
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

func (segmentOutput *SegmentOutputStream) handleFailureResponse(response protocol.Reply) bool {
	if response.GetType() == protocol.TypeWrongHost {
		segmentName := util.GetQualifiedStreamSegmentName(segmentOutput.segmentId)
		_, err := segmentOutput.sockets.RefreshMappingFor(segmentName, "")
		if err != nil {
			log.Errorf("Failed to refresh the host mapping for the segmentId: %v, will retry later. %v", segmentOutput.segmentId, err)
		}

		segmentOutput.dispatcher.Unregister(segmentOutput.requestId)
		segmentOutput.requestId = connection.NewFlow().AsLong()
		segmentOutput.dispatcher.Register(segmentOutput.requestId, segmentOutput.response)

		stop, err := segmentOutput.setupAppend()
		if stop || err != nil {
			return true
		}
		return false
	}
	if response.GetType() == protocol.TypeSegmentSealed {
		segmentOutput.close()
		segmentOutput.ControlCh <- &command.Command{Code: command.Resend, Message: segmentOutput.inflight}
		return true
	}
	if response.GetType() == protocol.TypeNoSuchSegment {
		// TODO: TransactionSegment
		segmentOutput.close()
		segmentOutput.ControlCh <- &command.Command{Code: command.Resend, Message: segmentOutput.inflight}
		return true
	}

	return true
}

func (segmentOutput *SegmentOutputStream) handleDataAppend(dataAppend *protocol.DataAppended) {
	//TODO:                checkAckLevels(ackLevel, previousAckLevel);
	//TODO:                State.noteSegmentLength(dataAppended.getCurrentSegmentWriteOffset());
	ackLevel := dataAppend.EventNumber
	segmentOutput.inflight.DeleteTo(canRemove, ackLevel)
}

func (segmentOutput *SegmentOutputStream) close() {
	segmentOutput.dispatcher.Unregister(segmentOutput.requestId)
	close(segmentOutput.response)
	segmentOutput.State = Stop
}

func (segmentOutput *SegmentOutputStream) closeWithResend() {
	segmentOutput.dispatcher.Unregister(segmentOutput.requestId)
	close(segmentOutput.response)
	segmentOutput.State = Stop
	segmentOutput.ControlCh <- &command.Command{Code: command.Resend, Message: segmentOutput.inflight}
}

func (segmentOutput *SegmentOutputStream) receiveResponse() {
	for response := range segmentOutput.response {
		if response.GetRequestId() != segmentOutput.requestId {
			log.Infof("Received the overdue the response: %v, ignore it", response)
		}
		segmentOutput.responseClient.Offer(response)
	}
}

func (segmentOutput *SegmentOutputStream) handleResponse() bool {
	// unblock, handler response from tcp
	response, _ := segmentOutput.responseClient.GetResponse(nil, connection.Now)
	if response != nil {
		if response.GetType() == protocol.TypeAppendSetup {
			log.Warning("received the overdue response: %v, ignore it", response)
		}
		if response.IsFailure() {
			stop := segmentOutput.handleFailureResponse(response)
			if stop {
				return stop
			}
			log.Errorf("hit error: %v, waiting for 500ms for recovery", response)
			time.Sleep(500 * time.Millisecond)
		}
		if response.GetType() == protocol.TypeDataAppended {
			dataAppended := response.(*protocol.DataAppended)
			segmentOutput.handleDataAppend(dataAppended)
		}
	}
	return false
}

func (segmentOutput *SegmentOutputStream) handleCommand() bool {
	// unblock, handler response from tcp
	select {
	case cmd := <-segmentOutput.ControlCh:
		if cmd.Code == command.Flush {
			for {
				if segmentOutput.inflight.Size == 0 {
					segmentOutput.ControlCh <- &command.Command{Code: command.Done}
					break
				}
				stop := segmentOutput.handleResponse()
				if stop {
					segmentOutput.closeWithResend()
					return true
				}
			}
		}
	default:
		break
	}
	return false
}

func (segmentOutput *SegmentOutputStream) doWrite() bool {
	select {
	case data := <-segmentOutput.WriteCh:

		stop, err := segmentOutput.write(data)
		if stop || err != nil {
			//restart policy to control here, when hit error, it means all connections are unavailable
			segmentOutput.closeWithResend()
			return true
		}
		break
	default:
		// flush if necessary
		if segmentOutput.handler.flushTime.Add(time.Second).Before(time.Now()) {
			err := segmentOutput.handler.flush()
			if err != nil {
				//restart policy to control here, when hit error, it means all connections are unavailable
				segmentOutput.closeWithResend()
				return true
			}
		}
		break
	}
	return false
}
func (segmentOutput *SegmentOutputStream) start() {
	for true {
		// block, handler command from upper layer
		stop := segmentOutput.handleCommand()
		if stop {
			break
		}
		// unblock, handler command from tcp
		stop = segmentOutput.handleResponse()
		if stop {
			break
		}

		// block write data
		stop = segmentOutput.doWrite()
		if stop {
			break
		}
	}
}
