package controller

import (
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	types "io.pravega.pravega-client-go/controller/proto"
)

type Controller struct {
	controllerClient types.ControllerServiceClient
	ctx              context.Context
}

func NewController(uri string) (*Controller, error) {
	conn, error := grpc.Dial(uri, grpc.WithInsecure())
	if error != nil {
		return nil, error
	}
	client := types.NewControllerServiceClient(conn)
	ctx := context.Background()
	return &Controller{
		controllerClient: client,
		ctx:              ctx,
	}, nil
}
func (controller *Controller) GetCurrentSegments(scope, streamName string) ([]types.SegmentId, error) {
	segments, err := controller.controllerClient.GetCurrentSegments(controller.ctx, &types.StreamInfo{
		Scope:  scope,
		Stream: streamName,
	})
	if err != nil {
		return nil, err
	}
	segmentIds := make([]types.SegmentId, len(segments.SegmentRanges))
	for i, segmentRange := range segments.SegmentRanges {
		segmentIds[i] = *segmentRange.SegmentId
	}
	return segmentIds, nil
}
func (controller *Controller) GetSegmentStoreURI(segmentId *types.SegmentId) (*types.NodeUri, error) {
	nodeUri, err := controller.controllerClient.GetURI(controller.ctx, segmentId)
	if err != nil {
		return nil, err
	}
	return nodeUri, nil
}
