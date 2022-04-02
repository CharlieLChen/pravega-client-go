package segment

import (
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"io.pravega.pravega-client-go/command"
	"io.pravega.pravega-client-go/connection"
	"io.pravega.pravega-client-go/controller"
	v1 "io.pravega.pravega-client-go/controller/proto"
	"io.pravega.pravega-client-go/protocol"
	"io.pravega.pravega-client-go/security/auth"
	"io.pravega.pravega-client-go/util"
	"sync/atomic"
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
	WriteCh        chan *protocol.PendingEvent
	BufferedCh     chan []byte
	stopCh         chan string
	activeInflight []*protocol.Append
	sealedInflight []*protocol.Append
	State          int32
	ControlCh      chan *command.Command
	flushResultCh  chan int
}

var (
	SetupUnCompleted = int32(0)
	SetupCompleted   = int32(1)
	Running          = int32(0)
	Close            = int32(1)
)

func NewSegmentOutputStream(segmentId *v1.SegmentId, controller *controller.ControllerImpl, control chan *command.Command, stopCh chan string, sockets *connection.Sockets) *SegmentOutputStream {
	writerId, _ := uuid.NewUUID()
	tokenProvider := &auth.EmptyDelegationTokenProvider{}
	requestId := connection.NewFlow().AsLong()

	segmentOutput := &SegmentOutputStream{
		segmentId:      segmentId,
		controller:     controller,
		writerId:       writerId,
		tokenProvider:  tokenProvider,
		requestId:      requestId,
		eventNumber:    0,
		sockets:        sockets,
		ControlCh:      control,
		setupStatus:    SetupUnCompleted,
		activeInflight: make([]*protocol.Append, 10),
		sealedInflight: nil,
		SegmentName:    util.GetQualifiedStreamSegmentName(segmentId),
		WriteCh:        make(chan *protocol.PendingEvent),
		BufferedCh:     make(chan []byte),
		flushResultCh:  make(chan int),
		stopCh:         stopCh,
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
	}

	return false, segmentOutput.send(append)

}
func (segmentOutput *SegmentOutputStream) InflightPendingEvent() []*protocol.PendingEvent {
	pendingEvent := make([]*protocol.PendingEvent, len(segmentOutput.sealedInflight)+len(segmentOutput.activeInflight))
	for _, e := range segmentOutput.sealedInflight {
		pendingEvent = append(pendingEvent, e.PendingEvent)
	}
	for _, e := range segmentOutput.activeInflight {
		pendingEvent = append(pendingEvent, e.PendingEvent)
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
			stop := segmentOutput.handleFailureResponse(response)
			return stop, nil
		}

		setup := response.(*protocol.AppendSetup)
		return false, segmentOutput.handleAppendSetup(setup)
	}

}

func (segmentOutput *SegmentOutputStream) handleAppendSetup(appendSetup *protocol.AppendSetup) error {
	log.Info("Received appendSetup {}", appendSetup)

	resend := append(segmentOutput.sealedInflight, segmentOutput.activeInflight...)
	segmentOutput.sealedInflight = nil
	segmentOutput.activeInflight = make([]*protocol.Append, len(resend))
	for _, element := range resend {
		if appendSetup.LastEventNumber < element.EventNumber {
			segmentOutput.activeInflight = append(segmentOutput.activeInflight, element)
			err := segmentOutput.send(element)
			if err != nil {
				return err
			}
		} else {
			element.PendingEvent.Future.Complete(util.Nothing)
		}
	}

	segmentOutput.setupStatus = SetupCompleted
	segmentOutput.handler.reset()

	return nil
}

func (segmentOutput *SegmentOutputStream) send(append *protocol.Append) error {
	send, err := segmentOutput.handler.SendAppend(append)
	if err != nil {
		return err
	}
	if send {
		segmentOutput.sendToBufferChannel()
	}
	return nil
}
func (segmentOutput *SegmentOutputStream) notifyFlushDone() {
	go func() { segmentOutput.flushResultCh <- 1 }()
}
func (segmentOutput *SegmentOutputStream) waitFlushDone() {
	start := time.Now()
	<-segmentOutput.flushResultCh
	waitDuration := int64(time.Now().Sub(start))

	bufferDuration := int64(segmentOutput.handler.flushTime.Sub(segmentOutput.handler.startTime))
	if bufferDuration == 0 {
		bufferDuration = 10
	}
	rate := float32(waitDuration) / float32(bufferDuration)
	if rate <= 0.01 {
		segmentOutput.handler.blockSize = (segmentOutput.handler.blockSize / 10) * 9
	}
	if rate > 0.2 {
		segmentOutput.handler.blockSize = int(float32(segmentOutput.handler.blockSize) * (1 + rate))
	}
	if segmentOutput.handler.blockSize > 7*1024*1024 {
		segmentOutput.handler.blockSize = 6 * 1024 * 1024
	}
}

func (segmentOutput *SegmentOutputStream) sendToBufferChannel() {
	buffered := segmentOutput.handler.Buffered()
	if len(buffered) == 0 {
		return
	}

	segmentOutput.waitFlushDone()

	segmentOutput.sealedInflight = segmentOutput.activeInflight
	segmentOutput.activeInflight = make([]*protocol.Append, 10)
	segmentOutput.BufferedCh <- buffered
	segmentOutput.handler.reset()
}

func (segmentOutput *SegmentOutputStream) handleFailureResponse(response protocol.Reply) bool {
	if response.GetType() == protocol.TypeWrongHost {
		segmentName := util.GetQualifiedStreamSegmentName(segmentOutput.segmentId)
		_, err := segmentOutput.sockets.RefreshMappingFor(segmentName, "")
		if err != nil {
			log.Errorf("Failed to refresh the host mapping for the segmentId: %v, will retry later. %v", segmentOutput.segmentId, err)
		}
		atomic.SwapInt32(&segmentOutput.setupStatus, SetupUnCompleted)
		return false
	}
	if response.GetType() == protocol.TypeSegmentSealed {
		return true
	}
	if response.GetType() == protocol.TypeNoSuchSegment {
		// TODO: TransactionSegment
		return true
	}

	return false
}

func (segmentOutput *SegmentOutputStream) handleDataAppend(dataAppend *protocol.DataAppended) {
	//TODO:                checkAckLevels(ackLevel, previousAckLevel);
	//TODO:                State.noteSegmentLength(dataAppended.getCurrentSegmentWriteOffset());
	//log.Infof("ACK event number: %d", dataAppend.EventNumber)
	last := segmentOutput.sealedInflight[len(segmentOutput.sealedInflight)-1]
	if last != nil {
		if last.EventNumber != dataAppend.EventNumber {
			log.Errorf("Miss data ")
			segmentOutput.State = Close
		}
		for _, e := range segmentOutput.sealedInflight {
			e.PendingEvent.Future.Complete(util.Nothing)
		}
		segmentOutput.sealedInflight = nil
	}

}

func (segmentOutput *SegmentOutputStream) close() {
	log.Infof("Close segment %s, event writer %s", segmentOutput.SegmentName, segmentOutput.writerId)
	segmentOutput.State = Close
	close(segmentOutput.BufferedCh)

	//unblock the network channel
	select {
	case <-segmentOutput.flushResultCh:
	default:
	}

	close(segmentOutput.flushResultCh)

	segmentOutput.stopCh <- segmentOutput.SegmentName

}

func (segmentOutput *SegmentOutputStream) handleCommand(cmd *command.Command) bool {
	if cmd.Code == command.Flush {
		segmentOutput.handler.bufferComplete()
		segmentOutput.sendToBufferChannel()
		for segmentOutput.sealedInflight != nil {
		}
		segmentOutput.ControlCh <- &command.Command{Code: command.Done}
		return false
	}
	if cmd.Code == command.Stop {
		segmentOutput.ControlCh <- &command.Command{Code: command.Done}
		return true
	}
	return false
}

func (segmentOutput *SegmentOutputStream) WriteToNetwork() {
	segmentOutput.notifyFlushDone()
	for data := range segmentOutput.BufferedCh {
		if segmentOutput.State == Close {
			break
		}
		reply, err := segmentOutput.sockets.Write(segmentOutput.SegmentName, data)
		if err != nil {
			log.Errorf("can't to write data to network %v", err)
			segmentOutput.State = Close
			segmentOutput.notifyFlushDone()
		}

		if reply.IsFailure() {
			stop := segmentOutput.handleFailureResponse(reply)
			if stop {
				segmentOutput.State = Close
				segmentOutput.notifyFlushDone()
			}
		}
		dataAppended := reply.(*protocol.DataAppended)
		segmentOutput.handleDataAppend(dataAppended)
		segmentOutput.notifyFlushDone()
	}
}
func (segmentOutput *SegmentOutputStream) start() {
	go segmentOutput.WriteToNetwork()
	segmentOutput.State = Running
	log.Infof("starting ")
	for {
		if segmentOutput.State == Close {
			segmentOutput.close()
			return
		}

		select {
		case cmd := <-segmentOutput.ControlCh:
			stop := segmentOutput.handleCommand(cmd)
			if stop {
				segmentOutput.close()
				return
			}
		case data := <-segmentOutput.WriteCh:

			segmentOutput.eventNumber = segmentOutput.eventNumber + 1
			newAppend := protocol.NewAppend(segmentOutput.segmentId, segmentOutput.writerId, segmentOutput.eventNumber, data, segmentOutput.requestId)
			segmentOutput.activeInflight = append(segmentOutput.activeInflight, newAppend)

			stop, err := segmentOutput.write(newAppend)
			if stop || err != nil {
				segmentOutput.close()
				return
			}
		case <-segmentOutput.handler.timer.C:
			log.Infof("flush data due to the timeout")
			segmentOutput.handler.bufferComplete()
			segmentOutput.sendToBufferChannel()
		}
	}

}
