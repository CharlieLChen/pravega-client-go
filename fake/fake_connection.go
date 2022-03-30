package fake

import (
	log "github.com/sirupsen/logrus"
	"io.pravega.pravega-client-go/protocol"
	"net"
)

const (
	Host = "127.0.0.1:8888"
	Port = 8888
)

type FakeServer struct {
	listener   net.Listener
	connection net.Conn
	stop       bool
}

func (fakeServer *FakeServer) CreateFakeServer() error {
	log.Info("start server")
	listen, err := net.Listen("tcp", Host)
	if err != nil {
		return err
	}
	fakeServer.listener = listen
	go func() {
		for !fakeServer.stop {
			con, _ := fakeServer.listener.Accept()
			fakeServer.connection = con
		}
	}()

	return nil
}
func (fakeServer *FakeServer) GetNextConnection() net.Conn {
	for fakeServer.connection == nil && !fakeServer.stop {
	}
	connection := fakeServer.connection
	fakeServer.connection = nil
	return connection

}

func (fakeServer *FakeServer) WriteHello() {
	go func() {
		connection := fakeServer.GetNextConnection()
		if fakeServer.stop {
			return
		}
		if connection != nil {
			connection.Write(protocol.HelloBytes)
		}
	}()

}

func (fakeServer *FakeServer) WriteAppendSetup() {
	go func() {
		connection := fakeServer.GetNextConnection()
		if fakeServer.stop {
			return
		}
		if connection != nil {
			connection.Write(protocol.HelloBytes)
			encoder := protocol.NewCommandEncoder()
			exists := protocol.NewSegmentExists(int64(10), "segment", "callstack")
			data := encoder.EncodeCommand(exists).Data()
			connection.Write(data)
		}
	}()

}

func (fakeServer *FakeServer) Close() {
	fakeServer.stop = true
	fakeServer.listener.Close()
}
