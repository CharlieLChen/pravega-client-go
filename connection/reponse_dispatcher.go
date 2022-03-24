package connection

import (
	log "github.com/sirupsen/logrus"
	"io.pravega.pravega-client-go/protocal"
	"sync"
)

type ResponseDispatcher struct {
	channels map[int64]chan protocal.Reply
	lock     sync.RWMutex
}

func (dispatcher *ResponseDispatcher) RegisterClient(requestId int64, response chan protocal.Reply) {
	dispatcher.lock.Lock()
	defer dispatcher.lock.Unlock()
	dispatcher.channels[requestId] = response
}
func (dispatcher *ResponseDispatcher) Dispatch(command protocal.WireCommand) {
	response, ok := command.(protocal.Reply)
	if !ok {
		log.Infof("Received the unrepliable response %v", command)
		return
	}

	requestId := response.GetRequestId()
	dispatcher.lock.RLock()
	defer dispatcher.lock.RUnlock()
	channel := dispatcher.channels[requestId]
	channel <- response
}
