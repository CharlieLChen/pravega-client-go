package controller

import (
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	types "io.pravega.pravega-client-go/controller/proto"
)

type Controller interface {
	GetCurrentSegments(scope, streamName string) ([]*types.SegmentRange, error)
	GetSegmentStoreURI(segmentId *types.SegmentId) (*types.NodeUri, error)
	CreateScope(scope string) error
	CreateStream(streamConfig *types.StreamConfig) error
}
type ControllerImpl struct {
	controllerClient types.ControllerServiceClient
	ctx              context.Context
}

func NewController(uri string) (*ControllerImpl, error) {
	conn, error := grpc.Dial(uri, grpc.WithInsecure())
	if error != nil {
		return nil, error
	}
	client := types.NewControllerServiceClient(conn)
	ctx := context.Background()
	return &ControllerImpl{
		controllerClient: client,
		ctx:              ctx,
	}, nil
}
func (controller *ControllerImpl) GetCurrentSegments(scope, streamName string) ([]*types.SegmentRange, error) {
	segments, err := controller.controllerClient.GetCurrentSegments(controller.ctx, &types.StreamInfo{
		Scope:  scope,
		Stream: streamName,
	})
	if err != nil {
		return nil, err
	}

	return segments.SegmentRanges, nil
}
func (controller *ControllerImpl) GetSegmentStoreURI(segmentId *types.SegmentId) (*types.NodeUri, error) {
	nodeUri, err := controller.controllerClient.GetURI(controller.ctx, segmentId)
	if err != nil {
		return nil, err
	}
	return nodeUri, nil
}

func (controller *ControllerImpl) CreateScope(scope string) error {
	createScopeStatus, err := controller.controllerClient.CreateScope(controller.ctx, &types.ScopeInfo{Scope: scope})
	if err != nil {
		return err
	}
	if createScopeStatus.Status == types.CreateScopeStatus_SUCCESS || createScopeStatus.Status == types.CreateScopeStatus_SCOPE_EXISTS {
		return nil
	}
	return fmt.Errorf("creating scope error code %v", createScopeStatus.Status)
}
func (controller *ControllerImpl) CreateStream(streamConfig *types.StreamConfig) error {
	createStreamStatus, err := controller.controllerClient.CreateStream(controller.ctx, streamConfig)
	if err != nil {
		return err
	}
	if createStreamStatus.Status == types.CreateStreamStatus_SUCCESS || createStreamStatus.Status == types.CreateStreamStatus_STREAM_EXISTS {
		return nil
	}
	return fmt.Errorf("creating stream error code %v", createStreamStatus.Status)
}
