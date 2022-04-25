package main

import (
	"flag"
	"fmt"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"io.pravega.pravega-client-go/connection"
	"io.pravega.pravega-client-go/controller"
	types "io.pravega.pravega-client-go/controller/proto"
	"io.pravega.pravega-client-go/stream"
	"os"
	"runtime/pprof"
	"time"
)

func main() {
	f, _ := os.Create("profile")
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()
	url := flag.String("url", "127.0.0.1:9090", "controller url")
	scope := flag.String("scope", "dell", "scope")
	streamName := flag.String("stream", "test", "stream")
	size := flag.Int("size", 1024*1024, "event size")
	count := flag.Int("count", 100, "event count")

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
	newController, err := controller.NewController(*url)
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
			TargetRate:     0,
			ScaleFactor:    0,
			MinNumSegments: 3,
		},
	}
	//duration = 10294
	// each event = 0.050263671875
	err = newController.CreateStream(config)
	if err != nil {
		log.Fatalf("%v", err)
	}

	sockets := connection.NewSockets(newController)
	streamWriter1 := stream.NewEventStreamWriter("dell", "test", newController, sockets)
	timestamps := time.Now()
	num := *count

	for i := 0; i < num; i++ {
		s := uuid.New().String()
		streamWriter1.WriteEvent(data, s)
	}
	streamWriter1.Flush()
	milliseconds := time.Now().Sub(timestamps).Milliseconds()
	fmt.Printf("cost time: %d milliseconds\n", milliseconds)
	fmt.Printf("each event: %f milliseconds\n", float64(milliseconds)/float64(num))
	if err != nil {
		log.Fatalf("%v", err)
	}
}
