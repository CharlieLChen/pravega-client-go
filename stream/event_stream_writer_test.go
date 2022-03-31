package stream

import (
	"fmt"
	"io.pravega.pravega-client-go/connection"
	"io.pravega.pravega-client-go/controller"
	types "io.pravega.pravega-client-go/controller/proto"
	"testing"
	"time"
)

func TestEventStreamWriter_WriteEvent(t *testing.T) {
	data := make([]byte, 1024)
	for i := range data {
		data[i] = 'a'
	}
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
			MinNumSegments: 1,
		},
	}
	err = newController.CreateStream(config)
	if err != nil {
		t.Fatalf("%v", err)
	}

	sockets := connection.NewSockets(newController)
	streamWriter1 := NewEventStreamWriter("dell", "test", newController, sockets)
	timestamps := time.Now()
	num := 200000
	for i := 0; i < num; i++ {
		streamWriter1.WriteEvent(data, "hello")
	}
	milliseconds := time.Now().Sub(timestamps).Milliseconds()
	fmt.Printf("cost time: %d milliseconds\n", milliseconds)
	fmt.Printf("each event: %f milliseconds\n", float64(milliseconds)/float64(num))
	if err != nil {
		t.Fatalf("%v", err)
	}
	time.Sleep(time.Second)
}
