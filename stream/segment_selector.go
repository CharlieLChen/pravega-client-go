package stream

import (
	log "github.com/sirupsen/logrus"
	"hash/maphash"
	"io.pravega.pravega-client-go/command"
	"io.pravega.pravega-client-go/connection"
	"io.pravega.pravega-client-go/controller"
	types "io.pravega.pravega-client-go/controller/proto"
	"io.pravega.pravega-client-go/protocol"
	"io.pravega.pravega-client-go/segment"
	"io.pravega.pravega-client-go/util"
	"sync"
	"time"
)

type SegmentSelector struct {
	scope               string
	stream              string
	controllerImp       *controller.ControllerImpl
	segmentIds          []*types.SegmentId
	writers             map[int]*segment.SegmentOutputStream
	writerStringMapping map[string]*segment.SegmentOutputStream
	hasher              *maphash.Hash
	sockets             *connection.Sockets
	segmentsStopChan    chan string
	EventCha            chan *protocol.PendingEvent
	lock                sync.Mutex
	timer               *time.Timer
}

func NewSegmentSelector(scope, stream string, controllerImp *controller.ControllerImpl, sockets *connection.Sockets) *SegmentSelector {
	hasher := new(maphash.Hash)
	m := map[int]*segment.SegmentOutputStream{}
	return &SegmentSelector{
		scope:               scope,
		stream:              stream,
		hasher:              hasher,
		controllerImp:       controllerImp,
		writers:             m,
		writerStringMapping: map[string]*segment.SegmentOutputStream{},
		sockets:             sockets,
		timer:               time.NewTimer(time.Minute),
		segmentsStopChan:    make(chan string),
	}
}
func (s *SegmentSelector) close() {
	close(s.EventCha)
	for _, writer := range s.writers {
		writer.ControlCh <- &command.Command{Code: command.Stop}
		//done
		<-writer.ControlCh
	}
}

func (s *SegmentSelector) write(event *protocol.PendingEvent) {
	writer, err := s.chooseSegmentWriter(event.RoutineKey)
	if err != nil {
		log.Errorf("can't to get segment writer: %v", err)
	}
	writer.WriteCh <- event
}
func (s *SegmentSelector) deleteSegment(segmentName string) {
	delete(s.writerStringMapping, segmentName)
	var key = -1
	for index, id := range s.segmentIds {
		name := util.GetQualifiedStreamSegmentName(id)
		if name == segmentName {
			key = index
			break
		}
	}
	if key == -1 {
		return
	}
	if key == 0 {
		s.segmentIds = s.segmentIds[1:]
	} else if key == len(s.segmentIds)-1 {
		s.segmentIds = s.segmentIds[0 : len(s.segmentIds)-2]
	} else {
		s.segmentIds = append(s.segmentIds[0:key], s.segmentIds[key+1:]...)
	}
	delete(s.writers, key)
}
func (s *SegmentSelector) start() {
	for {
		select {
		case segmentName := <-s.segmentsStopChan:
			writer := s.writerStringMapping[segmentName]
			event := writer.InflightPendingEvent()
			s.deleteSegment(segmentName)
			for _, e := range event {
				s.write(e)
			}
		case event := <-s.EventCha:
			s.write(event)
		case <-s.timer.C:
			s.refreshSegments()
		}
	}
}

func (selector *SegmentSelector) chooseSegmentWriter(routineKey string) (*segment.SegmentOutputStream, error) {
	_, err := selector.hasher.WriteString(routineKey)
	if err != nil {
		return nil, err
	}
	sum64 := selector.hasher.Sum64()
	selector.hasher.Reset()
	if selector.segmentIds == nil {
		err := selector.refreshSegments()
		if err != nil {
			return nil, err
		}
	}
	length := uint64(len(selector.segmentIds))
	i := int(sum64 % length)
	stream, ok := selector.writers[i]
	if ok {
		return stream, nil
	} else {
		controll := make(chan *command.Command)
		segmentOutputStream := segment.NewSegmentOutputStream(selector.segmentIds[i], selector.controllerImp, controll, selector.segmentsStopChan,
			selector.sockets)
		selector.writers[i] = segmentOutputStream
		selector.writerStringMapping[segmentOutputStream.SegmentName] = segmentOutputStream
		return selector.writers[i], nil
	}

}
func (selector *SegmentSelector) refreshSegments() error {
	segments, err := selector.controllerImp.GetCurrentSegments(selector.scope, selector.stream)
	if err != nil {
		return err
	}
	selector.segmentIds = segments
	return nil
}
