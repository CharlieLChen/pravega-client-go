package stream

import (
	"io.pravega.pravega-client-go/controller-client"
	types "io.pravega.pravega-client-go/controller-client/proto"
	"math/rand"
)

type SegmentSelector struct {
	controllerImp controller.Controller
	stream        string
	segmentIds    []types.SegmentId
}

func (selector *SegmentSelector) chooseSegment(scope, stream string) (*types.SegmentId, error) {
	if selector.segmentIds == nil {

		segments, err := selector.controllerImp.GetCurrentSegments(scope, stream)
		if err != nil {
			return nil, err
		}
		selector.segmentIds = segments
	}
	i := rand.Int() % len(selector.segmentIds)
	return &selector.segmentIds[i], nil
}
