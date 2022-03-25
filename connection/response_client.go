package connection

import (
	"io.pravega.pravega-client-go/errors"
	"io.pravega.pravega-client-go/protocol"
	"math"
	"time"
)

const (
	WaitEndless = math.MaxInt64
	Now         = 0
)

type ResponseClient struct {
	queue chan protocol.Reply
}

func NewResponseClient() *ResponseClient {
	replies := make(chan protocol.Reply, 1)
	return &ResponseClient{
		queue: replies,
	}
}
func (client *ResponseClient) Offer(res protocol.Reply) {
	client.queue <- res
}

func (client *ResponseClient) GetAppendSetup(timeout int64) (protocol.Reply, error) {
	return client.getResponse(protocol.TypeAppendSetup, timeout)
}

func (client *ResponseClient) getResponse(ty *protocol.WireCommandType, timeout int64) (protocol.Reply, error) {
	var duration time.Duration
	var getNow = false
	if timeout <= Now {
		getNow = true
	} else {
		i := (math.MaxInt64 / time.Second.Milliseconds()) / timeout
		//overflow
		if i < 1 {
			duration = (time.Duration)(math.MaxInt64)
		}
		duration = (time.Duration)(time.Second.Milliseconds() * timeout)
	}
	// nonblock
	if getNow {
		select {
		case res := <-client.queue:
			if res.GetType() != ty && res.IsFailure() {
				return res, errors.Error_Failure
			}
			return res, nil
		default:
			return nil, nil
		}
	}
	// block
	select {
	case res := <-client.queue:
		if res.GetType() != ty && res.IsFailure() {
			return res, errors.Error_Failure
		}
		return res, nil
	case <-time.After(duration):
		return nil, errors.Error_Timeout
	}
}
