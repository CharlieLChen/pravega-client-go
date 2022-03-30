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
	WriteCh        chan []byte
	BufferedCh     chan []byte
	actvieInflight *util.FIFOList
	sealedInflight *util.FIFOList
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
		actvieInflight: &util.FIFOList{},
		sealedInflight: nil,
		SegmentName:    util.GetQualifiedStreamSegmentName(segmentId),
		WriteCh:        make(chan []byte),
		BufferedCh:     make(chan []byte, 1024*1024),
	}
	segmentStoreHandler := NewAppendHandler(segmentOutput)
	segmentOutput.handler = segmentStoreHandler
	go segmentOutput.start()
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
	segmentOutput.actvieInflight.Insert(append)

	return false, segmentOutput.send(append)

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
	segmentOutput.handler.reset()

	actvieInflight := segmentOutput.actvieInflight
	sealedInflight := segmentOutput.sealedInflight
	segmentOutput.sealedInflight = nil
	segmentOutput.actvieInflight = &util.FIFOList{}

	if sealedInflight != nil {
		err := sealedInflight.ForEach(segmentOutput, resend, appendSetup.LastEventNumber)
		if err != nil {
			return err
		}
	}
	if actvieInflight != nil {
		err := actvieInflight.ForEach(segmentOutput, resend, appendSetup.LastEventNumber)
		if err != nil {
			return err
		}
	}

	segmentOutput.setupStatus = SetupCompleted
	return nil
}
func resend(handler interface{}, a interface{}, c interface{}) error {
	append := a.(*protocol.Append)
	segmentOutput := handler.(*SegmentOutputStream)
	eventNumber := c.(int64)
	if eventNumber < append.EventNumber {
		segmentOutput.actvieInflight.Insert(append)
		return segmentOutput.send(append)
	}
	return nil
}

func (segmentOutput *SegmentOutputStream) send(append *protocol.Append) error {
	send, err := segmentOutput.handler.SendAppend(append)
	if err != nil {
		return err
	}
	if send {
		segmentOutput.sendToBufferChannel()
		segmentOutput.handler.reset()
	}
	return nil
}

func (segmentOutput *SegmentOutputStream) sendToBufferChannel() {
	buffered := segmentOutput.handler.Buffered()
	if len(buffered) == 0 {
		return
	}
	for segmentOutput.sealedInflight != nil {
	}
	segmentOutput.sealedInflight = segmentOutput.actvieInflight
	segmentOutput.actvieInflight = &util.FIFOList{}
	copied := make([]byte, len(buffered))
	copy(copied, buffered)
	segmentOutput.BufferedCh <- copied
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
		segmentOutput.close()
		segmentOutput.ControlCh <- &command.Command{Code: command.Resend, Message: segmentOutput.actvieInflight}
		return true
	}
	if response.GetType() == protocol.TypeNoSuchSegment {
		// TODO: TransactionSegment
		segmentOutput.close()
		segmentOutput.ControlCh <- &command.Command{Code: command.Resend, Message: segmentOutput.actvieInflight}
		return true
	}

	return true
}

func (segmentOutput *SegmentOutputStream) handleDataAppend(dataAppend *protocol.DataAppended) {
	//TODO:                checkAckLevels(ackLevel, previousAckLevel);
	//TODO:                State.noteSegmentLength(dataAppended.getCurrentSegmentWriteOffset());
	//log.Infof("ACK event number: %d", dataAppend.EventNumber)
	last := segmentOutput.sealedInflight.GetLast()
	if last != nil {
		ap := last.(*protocol.Append)
		if ap.EventNumber != dataAppend.EventNumber {
			log.Errorf("Miss data ")
			segmentOutput.closeWithResend()
		}
		segmentOutput.sealedInflight = nil
	}

}

func (segmentOutput *SegmentOutputStream) close() {
	segmentOutput.State = Stop
}

func (segmentOutput *SegmentOutputStream) closeWithResend() {
	segmentOutput.State = Stop
	segmentOutput.ControlCh <- &command.Command{Code: command.Resend, Message: segmentOutput.actvieInflight}
}

func (segmentOutput *SegmentOutputStream) handleCommand() bool {
	// unblock, handler response from tcp
	select {
	case cmd := <-segmentOutput.ControlCh:
		if cmd.Code == command.Flush {
			err := segmentOutput.handler.bufferComplete()
			if err != nil {
				segmentOutput.closeWithResend()
			}
			segmentOutput.sendToBufferChannel()
			segmentOutput.handler.reset()
			for segmentOutput.sealedInflight != nil {
			}
			segmentOutput.ControlCh <- &command.Command{Code: command.Done}
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
		// bufferComplete if necessary
		if segmentOutput.handler.flushTime.Add(time.Second).Before(time.Now()) {
			err := segmentOutput.handler.bufferComplete()
			if err != nil {
				//restart policy to control here, when hit error, it means all connections are unavailable
				segmentOutput.closeWithResend()
				return true
			}
			segmentOutput.sendToBufferChannel()
			segmentOutput.handler.reset()
		}
		break
	}
	return false
}

func (segmentOutput *SegmentOutputStream) WriteToNetwork() {
	for data := range segmentOutput.BufferedCh {
		reply, err := segmentOutput.sockets.Write(segmentOutput.SegmentName, data)
		if err != nil {
			log.Errorf("can't to write data to network %v", err)
			segmentOutput.closeWithResend()
			break
		}

		if reply.IsFailure() {
			stop := segmentOutput.handleFailureResponse(reply)
			if stop {
				break
			}
		}
		dataAppended := reply.(*protocol.DataAppended)
		segmentOutput.handleDataAppend(dataAppended)

	}
}
func (segmentOutput *SegmentOutputStream) start() {
	go segmentOutput.WriteToNetwork()
	for true {
		// block, handler command from upper layer
		stop := segmentOutput.handleCommand()
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
