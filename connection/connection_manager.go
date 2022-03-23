package connection

import (
	"fmt"
	"io.pravega.pravega-client-go/controller"
	"io.pravega.pravega-client-go/util"
	"log"
	"sync"
)

type Sockets struct {
	lock               sync.Mutex
	pools              map[string]*ConnectionPool
	segmentNameMapping map[string]string
	rwLock             sync.RWMutex
	controller         *controller.Controller
}

func NewSockets() *Sockets {
	return &Sockets{
		pools:              map[string]*ConnectionPool{},
		segmentNameMapping: map[string]string{},
	}
}

func (sockets *Sockets) getSegmentStoreUrl(segmentName string) (string, error) {
	sockets.rwLock.RLock()
	url, ok := sockets.segmentNameMapping[segmentName]
	sockets.rwLock.RUnlock()
	if !ok {
		sockets.rwLock.Lock()
		defer sockets.rwLock.Unlock()
		url, ok := sockets.segmentNameMapping[segmentName]
		if ok {
			id, err := util.SegmentNameToId(segmentName)
			if err != nil {
				return "", err
			}
			uri, err := sockets.controller.GetSegmentStoreURI(id)
			if err != nil {
				return "", err
			}
			sockets.segmentNameMapping[segmentName] = fmt.Sprintf("%v:%v", uri.Endpoint, uri.Port)
			return sockets.segmentNameMapping[segmentName], nil
		}
		return url, nil
	}
	return url, nil
}

func (sockets *Sockets) refreshMappingFor(segmentName, original string) (string, error) {
	id, err := util.SegmentNameToId(segmentName)
	if err != nil {
		return "", err
	}
	uri, err := sockets.controller.GetSegmentStoreURI(id)
	if err != nil {
		return "", err
	}
	newUri := fmt.Sprintf("%v:%v", uri.Endpoint, uri.Port)
	if newUri != original {
		if original != "" {
			log.Printf("segmentName:%v node change from %v to %v\n", segmentName, original, newUri)
		}
		sockets.rwLock.Lock()
		defer sockets.rwLock.Unlock()
		sockets.segmentNameMapping[segmentName] = fmt.Sprintf("%v:%v", uri.Endpoint, uri.Port)
	}
	return newUri, nil
}

func (sockets *Sockets) Write(segmentName string, data []byte) (int, error) {
	url, err := sockets.getSegmentStoreUrl(segmentName)
	if err != nil {
		return 0, err
	}
	refreshedMapping := false
	for {
		connection, err := sockets.getConnection(url)
		// all connection are unavailable
		if err != nil {
			return 0, err
		}
		n, err := connection.TCPConn.Write(data)

		// the node may be down
		if err != nil {
			log.Println("WARNING: failed to write data, retrying with another connection")
			if refreshedMapping == false {
				url, err = sockets.refreshMappingFor(segmentName, url)
				if err != nil {
					return 0, err
				}
				refreshedMapping = true
			}
			connection.releaseWithFailure()
		}
		//successfully
		return n, nil
	}

}

func (sockets *Sockets) getConnection(url string) (*Connection, error) {
	pool, ok := sockets.pools[url]
	if ok {
		return pool.getConnection(url)
	}
	// memory barrier
	sockets.lock.Lock()
	defer sockets.lock.Unlock()
	pool, ok = sockets.pools[url]
	if ok {
		return pool.getConnection(url)
	} else {
		connectionPool := NewConnectionPool(10)
		sockets.pools[url] = connectionPool
		return connectionPool.getConnection(url)
	}
}
func (sockets *Sockets) releaseConnection(connection *Connection) {
	connection.release()
}
