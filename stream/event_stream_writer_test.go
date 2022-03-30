package stream

import (
	"io.pravega.pravega-client-go/connection"
	"io.pravega.pravega-client-go/controller"
	types "io.pravega.pravega-client-go/controller/proto"
	"testing"
	"time"
)

func TestEventStreamWriter_WriteEvent(t *testing.T) {
	data := []byte("hello world")
	newController, err := controller.NewController("localhost:9090")
	if err != nil {
		t.Fatalf("%v", err)
	}
	err = newController.CreateScope("dell")
	if err != nil {
		t.Fatalf("%v", err)
	}
	config := &types.StreamConfig{StreamInfo: &types.StreamInfo{
		Stream:          "test",
		Scope:           "dell",
		AccessOperation: types.StreamInfo_READ_WRITE},
		ScalingPolicy: &types.ScalingPolicy{
			ScaleType:      types.ScalingPolicy_FIXED_NUM_SEGMENTS,
			MinNumSegments: 3,
		},
	}
	err = newController.CreateStream(config)
	if err != nil {
		t.Fatalf("%v", err)
	}

	sockets := connection.NewSockets(newController)
	streamWriter1 := NewEventStreamWriter("dell", "test", newController, sockets)
	err = streamWriter1.WriteEvent(data, "hello")
	if err != nil {
		t.Fatalf("%v", err)
	}
	time.Sleep(time.Hour)
}
