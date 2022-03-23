package connection

import (
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
func (dispatcher *ResponseDispatcher) Dispatch(response protocal.Reply) {
	requestId := response.GetRequestId()
	dispatcher.lock.RLock()
	defer dispatcher.lock.RUnlock()
	channel := dispatcher.channels[requestId]
	channel <- response
}
