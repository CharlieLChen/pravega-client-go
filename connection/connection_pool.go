package connection

import (
	log "github.com/sirupsen/logrus"
	"io.pravega.pravega-client-go/errors"
	io_util "io.pravega.pravega-client-go/io"
	"io.pravega.pravega-client-go/protocal"
	"net"
	"sync"
	"sync/atomic"
)

type ConnectionPool struct {
	maxConnectionPerHost int
	connections          []*Connection
	lock                 sync.Mutex
	created              int
}

type Connection struct {
	url        string
	state      uint32
	TCPConn    *net.TCPConn
	index      int
	dispatcher *ResponseDispatcher
}

func (connect *Connection) release() {
	// atomic operation is enough for memory barrier
	atomic.SwapUint32(&connect.state, UnOccupied)
}

func (connect *Connection) parseResponseInternal() {
	typeBytes := make([]byte, 4)
	lengthBytes := make([]byte, 4)
	for {
		_, err := connect.TCPConn.Read(typeBytes)
		if err != nil {
			log.Errorf("Failed to read data from connection: %v", connect.url)
			connect.releaseWithFailure()
			return
		}
		_, err = connect.TCPConn.Read(lengthBytes)
		if err != nil {
			log.Errorf("Failed to read data from connection: %v", connect.url)
			connect.releaseWithFailure()
			return
		}
		types := io_util.BytestoInt32(typeBytes)
		length := io_util.BytestoInt32(lengthBytes)
		data := make([]byte, length)
		_, err = connect.TCPConn.Read(data)
		if err != nil {
			log.Errorf("Failed to read data from connection: %v", connect.url)
			connect.releaseWithFailure()
			return
		}
		commandType := protocal.TypesMapping[types]
		command, err := commandType.Factory.ReadFrom(data, length)
		if err != nil {
			log.Printf("Failed to parse response for commandType: %v", commandType)
		}
		connect.dispatcher.Dispatch(command)
	}
}
func (connect *Connection) parseResponse() {
	go connect.parseResponseInternal()
}

func (connect *Connection) releaseWithFailure() {
	// atomic operation is enough for memory barrier
	atomic.SwapUint32(&connect.state, Failed)
}
func NewConnectionPool(maxConnectionPerHost int) *ConnectionPool {
	return &ConnectionPool{
		maxConnectionPerHost: maxConnectionPerHost,
	}
}

func (pool *ConnectionPool) getConnection(url string) (*Connection, error) {
	pool.lock.Lock()
	defer pool.lock.Unlock()
	for _, connection := range pool.connections {
		if connection.state == UnOccupied {
			connection.state = Occupied
			return connection, nil
		}
	}

	if pool.created < pool.maxConnectionPerHost {
		conn, err := pool.createConnection(url)
		if err != nil {
			return nil, err
		}
		conn.state = Occupied
		return conn, nil
	}

	for {
		allFailed := true
		for _, connection := range pool.connections {
			if connection.state == UnOccupied {
				connection.state = Occupied
				return connection, nil
			}
			if connection.state != Failed {
				allFailed = false
			}
			if allFailed {
				return nil, errors.Error_All_Connections_Failed
			}
		}
	}
}

func (pool *ConnectionPool) createConnection(url string) (*Connection, error) {
	if pool.created >= pool.maxConnectionPerHost {
		return nil, nil
	}
	con, err := net.Dial("tcp", url)
	if err != nil {
		return nil, err
	}
	TCPConn := con.(*net.TCPConn)
	connection := &Connection{
		TCPConn: TCPConn,
		state:   UnOccupied,
	}
	pool.connections = append(pool.connections, connection)
	connection.index = len(pool.connections) - 1
	pool.created++
	return connection, err
}
