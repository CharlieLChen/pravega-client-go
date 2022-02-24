package main

import (
	"flag"
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	controller "io.pravega.pravega-client-go/controller/proto"
	"time"
)

var (
	addr = flag.String("addr", "localhost:9090", "the address to connect to")
	name = flag.String("name", "controller", "Name to greet")
)

func main() {
	flag.Parse()
	conn, _ := grpc.Dial(*addr, grpc.WithInsecure())
	defer conn.Close()
	client := controller.NewControllerServiceClient(conn)
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second)
	defer cancelFunc()
	scope, _ := client.ListScopes(ctx, &controller.ScopesRequest{})
	client.GetURI()
	fmt.Printf("%v\n", scope)
	segments, err := client.GetCurrentSegments(ctx, &controller.StreamInfo{Scope: "test", Stream: "test", AccessOperation: controller.StreamInfo_READ_WRITE})
	if err != nil {
		println(err)
	}
	fmt.Printf(segments.SegmentRanges)
}
