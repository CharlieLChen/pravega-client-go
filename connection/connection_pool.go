package connection

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"io.pravega.pravega-client-go/errors"
	"io.pravega.pravega-client-go/protocol"
	"net"
	"sync"
	"sync/atomic"
)

const (
	UnOccupied = uint32(0)
	Occupied   = uint32(1)
	Closed     = uint32(2)
	Failed     = uint32(10)
)

type ConnectionPool struct {
	maxConnectionPerHost int
	connections          []*Connection
	lock                 sync.Mutex
	created              int
}

func (pool *ConnectionPool) Close() {
	for _, connection := range pool.connections {
		connection.Close()
	}
}

type Connection struct {
	url        string
	state      uint32
	TCPConn    *net.TCPConn
	index      int
	dispatcher *ResponseDispatcher
}

func (connect *Connection) Close() {
	connect.state = Closed
	connect.TCPConn.Close()
	log.Infof("connection %d to %s closed", connect.index, connect.url)
}

func (connect *Connection) Write(data []byte) (protocol.Reply, error) {
	_, err := connect.TCPConn.Write(data)
	if err != nil {
		return nil, err
	}
	decode, err := protocol.Decode(connect.TCPConn)
	if err != nil {
		return nil, err
	}
	reply, ok := decode.(protocol.Reply)
	if !ok {
		return nil, fmt.Errorf("not a reply command %v", decode)
	}
	return reply, err
}

func (connect *Connection) release() {
	// atomic operation is enough for memory barrier
	atomic.SwapUint32(&connect.state, UnOccupied)
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
func (pool *ConnectionPool) displayConnection() {
	for _, con := range pool.connections {
		log.Infof("url:%s, index: %d, status: %d", con.url, con.index, con.state)
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
		}
		if allFailed {
			pool.displayConnection()
			return nil, errors.Error_All_Connections_Failed
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
	_, err = TCPConn.Write(protocol.HelloBytes)
	if err != nil {
		log.Errorf("Failed to create connection for %v, %v", url, err)
		return nil, err
	}
	decode, err := protocol.Decode(TCPConn)
	if err != nil {
		log.Errorf("Failed to create connection for %v, %v", url, err)
		return nil, err
	}
	if decode.GetType() != protocol.TypeHello {
		log.Errorf("get unexpected response when create connection %v, %v", url, decode.GetType())
		return nil, err
	}

	connection := &Connection{
		TCPConn: TCPConn,
		state:   UnOccupied,
		url:     url,
	}
	pool.connections = append(pool.connections, connection)
	connection.index = len(pool.connections) - 1
	pool.created++

	return connection, err
}
