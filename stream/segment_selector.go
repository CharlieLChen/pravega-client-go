package stream

import (
	log "github.com/sirupsen/logrus"
	"github.com/spaolacci/murmur3"
	"io.pravega.pravega-client-go/command"
	"io.pravega.pravega-client-go/connection"
	"io.pravega.pravega-client-go/controller"
	types "io.pravega.pravega-client-go/controller/proto"
	io_util "io.pravega.pravega-client-go/io"
	"io.pravega.pravega-client-go/protocol"
	"io.pravega.pravega-client-go/segment"
	"io.pravega.pravega-client-go/util"
	"sync"
)

type SegmentSelector struct {
	scope               string
	stream              string
	controllerImp       *controller.ControllerImpl
	segmentWithRange    []*types.SegmentRange
	writers             map[int]*segment.SegmentOutputStream
	writerStringMapping map[string]*segment.SegmentOutputStream
	hasher              murmur3.Hash128
	sockets             *connection.Sockets
	segmentsStopChan    chan string
	EventCha            chan *protocol.PendingEvent
	CommandCha          chan *command.Command
	lock                sync.Mutex
}

func NewSegmentSelector(scope, stream string, controllerImp *controller.ControllerImpl, sockets *connection.Sockets) *SegmentSelector {
	hasher := murmur3.New128WithSeed(util.Seed)
	m := map[int]*segment.SegmentOutputStream{}
	selector := SegmentSelector{
		scope:               scope,
		stream:              stream,
		hasher:              hasher,
		controllerImp:       controllerImp,
		writers:             m,
		writerStringMapping: map[string]*segment.SegmentOutputStream{},
		sockets:             sockets,
		segmentsStopChan:    make(chan string, 100),
		EventCha:            make(chan *protocol.PendingEvent),
		CommandCha:          make(chan *command.Command),
	}
	go selector.start()
	return &selector

}
func (selector *SegmentSelector) close() {
	close(selector.EventCha)
	for _, writer := range selector.writers {
		writer.ControlCh <- &command.Command{Code: command.Stop}
		//done
		<-writer.ControlCh
	}
}

func (selector *SegmentSelector) write(event *protocol.PendingEvent) {
	writer, err := selector.chooseSegmentWriter(event.RoutineKey)
	if err != nil {
		log.Errorf("can't to get segment writer: %v", err)
	}
	writer.WriteCh <- event
}

func (selector *SegmentSelector) deleteSegment(segmentName string) {
	delete(selector.writerStringMapping, segmentName)
	var key = -1
	for index, id := range selector.segmentWithRange {
		key++
		name := util.GetQualifiedStreamSegmentName(id.SegmentId)
		if name == segmentName {
			key = index
			break
		}
	}
	if key == -1 {
		return
	}
	if key == 0 {
		selector.segmentWithRange = selector.segmentWithRange[1:]
	} else if key == len(selector.segmentWithRange)-1 {
		selector.segmentWithRange = selector.segmentWithRange[0 : len(selector.segmentWithRange)-2]
	} else {
		selector.segmentWithRange = append(selector.segmentWithRange[0:key], selector.segmentWithRange[key+1:]...)
	}
	delete(selector.writers, key)
}
func (selector *SegmentSelector) start() {
	for {
		select {
		case segmentName := <-selector.segmentsStopChan:
			log.Infof("received the closing signal from %s", segmentName)
			writer := selector.writerStringMapping[segmentName]
			event := writer.InflightPendingEvent()
			selector.deleteSegment(segmentName)
			selector.refreshSegments()
			for _, e := range event {
				selector.write(e)
			}
		case event := <-selector.EventCha:
			selector.write(event)
		case command := <-selector.CommandCha:
			selector.handleCommand(command)
		}
	}
}
func (selector *SegmentSelector) handleCommand(cmd *command.Command) {
	switch cmd.Code {
	case command.Flush:
		for _, writer := range selector.writers {
			writer.ControlCh <- &command.Command{Code: command.Flush}
			<-writer.ControlCh
		}
		selector.CommandCha <- &command.Command{Code: command.Done}
	}
}

func (selector *SegmentSelector) chooseSegmentWriter(routineKey string) (*segment.SegmentOutputStream, error) {
	data := []byte(routineKey)
	_, err := selector.hasher.Write(data)
	if err != nil {
		return nil, err
	}
	upper, _ := selector.hasher.Sum128()
	selector.hasher.Reset()
	if selector.segmentWithRange == nil {
		err := selector.refreshSegments()
		if err != nil {
			return nil, err
		}
	}
	toFloat64 := io_util.Int64ToFloat64(upper)
	for i, segmentRange := range selector.segmentWithRange {
		if segmentRange.MinKey < toFloat64 && toFloat64 < segmentRange.MaxKey {
			writer, ok := selector.writers[i]
			if ok {
				return writer, nil
			}
			segmentOutputStream := segment.NewSegmentOutputStream(segmentRange.SegmentId, selector.controllerImp, selector.segmentsStopChan,
				selector.sockets)
			selector.writers[i] = segmentOutputStream
			selector.writerStringMapping[segmentOutputStream.SegmentName] = segmentOutputStream
			return selector.writers[i], nil
		}
	}
	return nil, err

}
func (selector *SegmentSelector) refreshSegments() error {
	segments, err := selector.controllerImp.GetCurrentSegments(selector.scope, selector.stream)
	if err != nil {
		return err
	}
	selector.segmentWithRange = segments
	log.Infof("refreshed the segments")
	return nil
}
