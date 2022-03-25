package connection

import (
	log "github.com/sirupsen/logrus"
	"io.pravega.pravega-client-go/connection/response"
	"io.pravega.pravega-client-go/protocol"
	"sync"
)

type ResponseDispatcher struct {
	channels map[int64]chan protocol.Reply
	lock     sync.RWMutex
}

func (dispatcher *ResponseDispatcher) RegisterClient(requestId int64, response chan protocol.Reply) {
	dispatcher.lock.Lock()
	defer dispatcher.lock.Unlock()
	dispatcher.channels[requestId] = response
}
func (dispatcher *ResponseDispatcher) Dispatch(command protocol.WireCommand) {
	if command.GetType() == protocol.TypeHello {
		response.Hello(command.(*protocol.Hello))
	}
	if command.GetType() == protocol.TypeKeepAlive {
		response.Hello(command.(*protocol.Hello))
	}

	res, ok := command.(protocol.Reply)
	if !ok {
		log.Infof("Received the unrepliable response %v", command)
		return
	}

	requestId := res.GetRequestId()
	dispatcher.lock.RLock()
	defer dispatcher.lock.RUnlock()
	channel := dispatcher.channels[requestId]
	channel <- res
}
