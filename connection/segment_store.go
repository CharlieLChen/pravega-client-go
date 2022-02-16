package connection

import (
	"fmt"
	"io.pravega.pravega-client-go/protocal"
	"net"
)

func main() {
	segmentstore, err := net.Dial("tcp", "localhost:6000")
	if err != nil {
		println(err.Error())
	}
	hello := protocal.NewHello(100, 4)
	encoder := NewCommandEncoder()
	msg := encoder.Write(hello)
	write, err := segmentstore.Write(msg.Data())

	fmt.Println(msg.Data())
	print("\n")
	if err != nil {
		println(err.Error())
	}
	print(write)
	b := make([]byte, 16)
	read, err := segmentstore.Read(b)
	fmt.Printf("\nreads: %v\n", read)
	fmt.Printf("\ncontent: %v\n", b)
}
