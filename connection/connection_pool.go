package connection

import (
	log "github.com/sirupsen/logrus"
	"io.pravega.pravega-client-go/errors"
	"io.pravega.pravega-client-go/protocol"
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
	atomic.SwapUint32(&connect.state, protocol.UnOccupied)
}

func (connect *Connection) parseResponseInternal() {
	for {
		decode, err := protocol.Decode(connect.TCPConn)
		if err != nil {
			log.Printf("Failed to parse response for command %v", err)
			connect.releaseWithFailure()
		}
		connect.dispatcher.Dispatch(decode)
	}
}
func (connect *Connection) parseResponse() {
	go connect.parseResponseInternal()
}

func (connect *Connection) releaseWithFailure() {
	// atomic operation is enough for memory barrier
	atomic.SwapUint32(&connect.state, protocol.Failed)
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
		if connection.state == protocol.UnOccupied {
			connection.state = protocol.Occupied
			return connection, nil
		}
	}

	if pool.created < pool.maxConnectionPerHost {
		conn, err := pool.createConnection(url)
		if err != nil {
			return nil, err
		}
		conn.state = protocol.Occupied
		return conn, nil
	}

	for {
		allFailed := true
		for _, connection := range pool.connections {
			if connection.state == protocol.UnOccupied {
				connection.state = protocol.Occupied
				return connection, nil
			}
			if connection.state != protocol.Failed {
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
		state:   protocol.UnOccupied,
	}
	pool.connections = append(pool.connections, connection)
	connection.index = len(pool.connections) - 1
	pool.created++
	return connection, err
}
