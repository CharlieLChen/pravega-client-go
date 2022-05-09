package segment

import (
	"fmt"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"io.pravega.pravega-client-go/command"
	"io.pravega.pravega-client-go/connection"
	"io.pravega.pravega-client-go/controller"
	v1 "io.pravega.pravega-client-go/controller/proto"
	"io.pravega.pravega-client-go/protocol"
	"io.pravega.pravega-client-go/security/auth"
	"io.pravega.pravega-client-go/util"
)

type SegmentOutputStream struct {
	segmentId     *v1.SegmentId
	SegmentName   string
	controller    *controller.ControllerImpl
	writerId      uuid.UUID
	tokenProvider *auth.EmptyDelegationTokenProvider
	requestId     int64
	sockets       *connection.Sockets
	eventNumber   int64
	handler       *AppendHandler
	setupStatus   int32
	WriteCh       chan *protocol.PendingEvent
	stopCh        chan string
	inflight      []*protocol.Append
	State         int32
	ControlCh     chan *command.Command
}

var (
	SetupUnCompleted = int32(0)
	SetupCompleted   = int32(1)
	Running          = int32(0)
	Close            = int32(1)
)

func NewSegmentOutputStream(segmentId *v1.SegmentId, controller *controller.ControllerImpl, stopCh chan string, sockets *connection.Sockets) *SegmentOutputStream {
	writerId, _ := uuid.NewUUID()
	tokenProvider := &auth.EmptyDelegationTokenProvider{}
	requestId := connection.NewFlow().AsLong()

	segmentOutput := &SegmentOutputStream{
		segmentId:     segmentId,
		controller:    controller,
		writerId:      writerId,
		tokenProvider: tokenProvider,
		requestId:     requestId,
		eventNumber:   0,
		sockets:       sockets,
		ControlCh:     make(chan *command.Command),
		setupStatus:   SetupUnCompleted,
		inflight:      make([]*protocol.Append, 0),
		SegmentName:   util.GetQualifiedStreamSegmentName(segmentId),
		WriteCh:       make(chan *protocol.PendingEvent),
		stopCh:        stopCh,
	}
	segmentStoreHandler := NewAppendHandler(segmentOutput)
	segmentOutput.handler = segmentStoreHandler
	go segmentOutput.start()
	return segmentOutput
}

func (segmentOutput *SegmentOutputStream) write(append *protocol.Append) (bool, error) {
	if segmentOutput.setupStatus == SetupUnCompleted {
		stop, err := segmentOutput.setupAppend()
		if err != nil {
			return stop, err
		}
		if stop {
			return true, nil
		}
		return false, nil
	} else {
		return segmentOutput.send(append)
	}
}

func (segmentOutput *SegmentOutputStream) InflightPendingEvent() []*protocol.PendingEvent {
	pendingEvent := make([]*protocol.PendingEvent, len(segmentOutput.inflight))
	for i, e := range segmentOutput.inflight {
		pendingEvent[i] = e.PendingEvent
	}
	return pendingEvent
}

func (segmentOutput *SegmentOutputStream) setupAppend() (bool, error) {
	segmentName := util.GetQualifiedStreamSegmentName(segmentOutput.segmentId)
	token := segmentOutput.tokenProvider.RetrieveToken()
	setupAppend := protocol.NewSetupAppend(segmentOutput.requestId, segmentOutput.writerId, segmentName, token)
	for {
		response, err := segmentOutput.handler.SendCommand(setupAppend)
		if err != nil {
			return true, err
		}

		if response.IsFailure() {
			return segmentOutput.handleFailureResponse(response)
		}

		setup := response.(*protocol.AppendSetup)
		return segmentOutput.handleAppendSetup(setup)
	}

}

func (segmentOutput *SegmentOutputStream) handleAppendSetup(appendSetup *protocol.AppendSetup) (bool, error) {
	log.Infof("Received appendSetup {%v}", appendSetup)

	resend := segmentOutput.inflight

	segmentOutput.inflight = make([]*protocol.Append, 0)
	for _, element := range resend {
		if appendSetup.LastEventNumber < element.EventNumber {
			segmentOutput.inflight = append(segmentOutput.inflight, element)
			stop, err := segmentOutput.send(element)
			if err != nil {
				return stop, err
			}

		} else {
			element.PendingEvent.Future.Complete(util.Nothing)
		}
	}

	segmentOutput.setupStatus = SetupCompleted
	segmentOutput.handler.reset()

	return false, nil
}

func (segmentOutput *SegmentOutputStream) send(append *protocol.Append) (bool, error) {
	full, err := segmentOutput.handler.bufferEvent(append)
	if err != nil {
		return false, err
	}
	if full {
		return segmentOutput.sendToNetwork()
	}
	return false, nil
}

func (segmentOutput *SegmentOutputStream) sendToNetwork() (bool, error) {
	buffered := segmentOutput.handler.Buffered()
	if len(buffered) == 0 {
		return false, nil
	}

	reply, err := segmentOutput.sockets.Write(segmentOutput.SegmentName, buffered)
	if err != nil {
		log.Errorf("can't to write data to network %v", err)
		return false, err
	}

	if reply.IsFailure() {
		return segmentOutput.handleFailureResponse(reply)
	}
	dataAppended := reply.(*protocol.DataAppended)
	segmentOutput.handleDataAppend(dataAppended)
	segmentOutput.handler.reset()
	return false, nil
}

func (segmentOutput *SegmentOutputStream) handleFailureResponse(response protocol.Reply) (bool, error) {
	if response.GetType() == protocol.TypeWrongHost {
		segmentName := util.GetQualifiedStreamSegmentName(segmentOutput.segmentId)
		_, err := segmentOutput.sockets.RefreshMappingFor(segmentName, "")
		if err != nil {
			log.Errorf("Failed to refresh the host mapping for the segmentId: %v, will retry later. %v", segmentOutput.segmentId, err)
		}
		segmentOutput.setupStatus = SetupUnCompleted
		return false, err
	}
	if response.GetType() == protocol.TypeSegmentSealed {
		return true, nil
	}
	if response.GetType() == protocol.TypeNoSuchSegment {
		// TODO: TransactionSegment
		return true, nil
	}

	return false, nil
}

func (segmentOutput *SegmentOutputStream) handleDataAppend(dataAppend *protocol.DataAppended) {
	//TODO:                checkAckLevels(ackLevel, previousAckLevel);
	//TODO:                State.noteSegmentLength(dataAppended.getCurrentSegmentWriteOffset());
	log.Infof("writer: %s, event number: %d", segmentOutput.writerId, dataAppend.EventNumber)
	last := segmentOutput.inflight[len(segmentOutput.inflight)-1]
	if last != nil {
		if last.EventNumber != dataAppend.EventNumber {
			err := fmt.Errorf("Miss data: expected EventNumber: %v, actually got: %v ", last.EventNumber, dataAppend.EventNumber)
			panic(err)
		}
		for _, e := range segmentOutput.inflight {
			e.PendingEvent.Future.Complete(util.Nothing)
		}
		segmentOutput.inflight = nil
	}
}

func (segmentOutput *SegmentOutputStream) close() {
	log.Infof("Closing segment %s, event writer %s", segmentOutput.SegmentName, segmentOutput.writerId)
	segmentOutput.State = Close
	segmentOutput.stopCh <- segmentOutput.SegmentName
	log.Infof("Closed segment %s, event writer %s", segmentOutput.SegmentName, segmentOutput.writerId)
}

func (segmentOutput *SegmentOutputStream) handleCommand(cmd *command.Command) (bool, error) {
	if cmd.Code == command.Flush {
		stop, err := segmentOutput.flush()
		segmentOutput.ControlCh <- &command.Command{Code: command.Done}
		log.Infof("Flush done")
		return stop, err
	}
	if cmd.Code == command.Stop {
		_, err := segmentOutput.flush()
		return true, err
	}
	return false, nil
}

func (segmentOutput *SegmentOutputStream) flush() (bool, error) {
	segmentOutput.handler.bufferComplete()
	stop, err := segmentOutput.sendToNetwork()
	if stop || err != nil {
		return stop, err
	}
	segmentOutput.handler.reset()
	return false, nil
}
func (segmentOutput *SegmentOutputStream) start() {
	segmentOutput.State = Running
	for {
		if segmentOutput.State == Close {
			segmentOutput.close()
			return
		}

		select {
		case cmd := <-segmentOutput.ControlCh:
			stop, err := segmentOutput.handleCommand(cmd)
			if stop {
				segmentOutput.close()
				return
			}
			if err != nil {
				_, err := segmentOutput.setupAppend()
				log.Errorf("can't setup append due to %v", err)
				segmentOutput.close()
				return
			}
		case data := <-segmentOutput.WriteCh:
			segmentOutput.eventNumber = segmentOutput.eventNumber + 1
			newAppend := protocol.NewAppend(segmentOutput.segmentId, segmentOutput.writerId, segmentOutput.eventNumber, data, segmentOutput.requestId)
			segmentOutput.inflight = append(segmentOutput.inflight, newAppend)
			stop, err := segmentOutput.write(newAppend)
			if stop {
				segmentOutput.close()
				return
			}
			if err != nil {
				log.Errorf("can't write event due to %v, try to setup append.", err)
				_, err := segmentOutput.setupAppend()
				log.Errorf("can't setup append due to %v", err)
				segmentOutput.close()
				return
			}
		case <-segmentOutput.handler.timer.C:
			log.Infof("flush data due to the timeout")
			if segmentOutput.setupStatus == SetupUnCompleted {
				segmentOutput.handler.reset()
				break
			}
			stop, err := segmentOutput.flush()
			if stop {
				segmentOutput.close()
				return
			}
			if err != nil {
				_, err := segmentOutput.setupAppend()
				log.Errorf("can't setup append %v", err)
				segmentOutput.close()
				return
			}
			segmentOutput.handler.reset()
		}
	}

}
