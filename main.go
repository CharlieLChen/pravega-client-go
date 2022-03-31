package main

import (
	"flag"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io.pravega.pravega-client-go/connection"
	"io.pravega.pravega-client-go/controller"
	types "io.pravega.pravega-client-go/controller/proto"
	"io.pravega.pravega-client-go/stream"
	"time"
)

func main() {
	url := flag.String("url", "localhost:9090", "controller url")
	scope := flag.String("scope", "dell", "scope")
	streamName := flag.String("stream", "test", "stream")
	size := flag.Int("size", 1024*1024, "event size")
	count := flag.Int("count", 1024, "event count")

	flag.Parse()
	fmt.Println("url:", *url)
	fmt.Println("scope:", *scope)
	fmt.Println("stream:", *streamName)
	fmt.Println("size:", *size)
	fmt.Println("count:", *count)
	data := make([]byte, *size)
	for i := range data {
		data[i] = 'a'
	}
	newController, err := controller.NewController("localhost:9090")
	if err != nil {
		log.Fatalf("%v", err)
	}
	err = newController.CreateScope("dell")
	if err != nil {
		log.Fatalf("%v", err)
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
		log.Fatalf("%v", err)
	}

	sockets := connection.NewSockets(newController)
	streamWriter1 := stream.NewEventStreamWriter("dell", "test", newController, sockets)
	timestamps := time.Now()
	num := *count
	for i := 0; i < num; i++ {
		streamWriter1.WriteEvent(data, "hello")
	}
	milliseconds := time.Now().Sub(timestamps).Milliseconds()
	fmt.Printf("cost time: %d milliseconds\n", milliseconds)
	fmt.Printf("each event: %f milliseconds\n", float64(milliseconds)/float64(num))
	if err != nil {
		log.Fatalf("%v", err)
	}
	time.Sleep(time.Second)
}
