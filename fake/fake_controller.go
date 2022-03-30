package fake

import (
	"fmt"
	types "io.pravega.pravega-client-go/controller/proto"
	v1 "io.pravega.pravega-client-go/controller/proto"
	"io.pravega.pravega-client-go/util"
)

type FakeController struct {
	UriMapping map[string]*types.NodeUri
	SegmentIds []*types.SegmentId
}

func NewFakeController() *FakeController {
	controller := &FakeController{}
	controller.SegmentIds = []*types.SegmentId{
		{
			StreamInfo: &v1.StreamInfo{
				Scope:  "test",
				Stream: "test",
			},
			SegmentId: 0,
		},
		{
			StreamInfo: &v1.StreamInfo{
				Scope:  "test",
				Stream: "test",
			},
			SegmentId: 1,
		},
		{
			StreamInfo: &v1.StreamInfo{
				Scope:  "test",
				Stream: "test",
			},
			SegmentId: 2,
		},
	}
	controller.UriMapping = map[string]*types.NodeUri{}
	for i, id := range controller.SegmentIds {
		name := util.GetQualifiedStreamSegmentName(id)
		controller.UriMapping[name] = &types.NodeUri{
			Endpoint: fmt.Sprintf("127.0.0.%d", i),
			Port:     8888,
		}
	}
	return controller
}
func (controller *FakeController) GetCurrentSegments(scope, streamName string) ([]*types.SegmentId, error) {
	return controller.SegmentIds, nil
}
func (controller *FakeController) GetSegmentStoreURI(segmentId *types.SegmentId) (*types.NodeUri, error) {
	name := util.GetQualifiedStreamSegmentName(segmentId)
	return controller.UriMapping[name], nil
}
func (receiver *FakeController) CreateScope(scope string) error {
	return nil
}
func (receiver *FakeController) CreateStream(streamConfig *types.StreamConfig) error {
	return nil
}
