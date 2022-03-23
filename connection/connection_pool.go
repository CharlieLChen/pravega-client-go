package connection

import (
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
	state   uint32
	TCPConn *net.TCPConn
	index   int
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
		for _, connection := range pool.connections {
			if connection.state == UnOccupied {
				connection.state = Occupied
				return connection, nil
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
