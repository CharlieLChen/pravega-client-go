package connection

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"io.pravega.pravega-client-go/controller"
	"io.pravega.pravega-client-go/protocol"
	"io.pravega.pravega-client-go/util"
	"sync"
)

type Sockets struct {
	lock               sync.Mutex
	pools              map[string]*ConnectionPool
	segmentNameMapping map[string]string
	rwLock             sync.RWMutex
	controller         controller.Controller
}

func NewSockets(controller controller.Controller) *Sockets {
	return &Sockets{
		pools:              map[string]*ConnectionPool{},
		segmentNameMapping: map[string]string{},
		controller:         controller,
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
		if !ok {
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

func (sockets *Sockets) RefreshMappingFor(segmentName, original string) (string, error) {
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

func (sockets *Sockets) Write(segmentName string, data []byte) (protocol.Reply, error) {
	url, err := sockets.getSegmentStoreUrl(segmentName)
	if err != nil {
		return nil, err
	}

	for {
		connection, err := sockets.getConnection(url)
		// all connection are unavailable
		if err != nil {
			log.Errorf("All connections for %s are unavaiable, %v", url, err)
			return nil, err
		}
		response, err := connection.Write(data)

		// the node may be down
		if err != nil {
			log.Errorf("failed to write data due to %v, retrying with another connection", err)
			url, err = sockets.RefreshMappingFor(segmentName, url)
			if err != nil {
				return nil, err
			}

			connection.releaseWithFailure()
			continue
		}
		connection.release()
		//successfully
		return response, nil
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
