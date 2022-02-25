package stream

import (
	"hash/maphash"
	"io.pravega.pravega-client-go/controller"
	types "io.pravega.pravega-client-go/controller/proto"
	"io.pravega.pravega-client-go/segment"
)

type SegmentSelector struct {
	scope         string
	stream        string
	controllerImp *controller.Controller
	segmentIds    []types.SegmentId
	writers       map[int]*segment.SegmentOutputStream
	hasher        *maphash.Hash
}

func NewSegmentSelector(scope, stream string, controllerImp *controller.Controller) *SegmentSelector {
	hasher := new(maphash.Hash)
	m := map[int]*segment.SegmentOutputStream{}
	return &SegmentSelector{
		scope:         scope,
		stream:        stream,
		hasher:        hasher,
		controllerImp: controllerImp,
		writers:       m,
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
		selector.writers[i] = segment.NewSegmentOutputStream(&selector.segmentIds[i], selector.controllerImp)
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
