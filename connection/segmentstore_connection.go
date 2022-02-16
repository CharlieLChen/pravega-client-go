package connection

import (
	"fmt"
	"net"
)

type SegmentStoreConnection struct {
	address    string
	port       int32
	connection *net.TCPConn
	encoder    *CommandEncoder
}

func NewSegmentStoreConnection(address string, port int32) (*SegmentStoreConnection, error) {
	connection := &SegmentStoreConnection{
		address: address,
		port:    port,
	}
	url := fmt.Sprintf("%s:%v", address, port)
	con, err := net.Dial("tcp", url)
	if err != nil {
		return nil, err
	}
	connection.connection = con.(*net.TCPConn)
	connection.encoder = NewCommandEncoder()
	return connection, nil
}
