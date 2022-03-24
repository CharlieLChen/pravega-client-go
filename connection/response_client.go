package connection

import (
	"io.pravega.pravega-client-go/errors"
	"io.pravega.pravega-client-go/protocal"
	"math"
	"time"
)

const (
	WaitEndless = 0
)

type ResponseClient struct {
	queue chan protocal.Reply
}

func NewResponseClient() *ResponseClient {
	replies := make(chan protocal.Reply, 1)
	return &ResponseClient{
		queue: replies,
	}
}
func (client *ResponseClient) Offer(res protocal.Reply) {
	client.queue <- res
}

func (client *ResponseClient) GetAppendSetup(timeout int64) (protocal.Reply, error) {
	return client.getResponse(protocal.WirecommandtypeAppendSetup, timeout)
}

func (client *ResponseClient) getResponse(ty *protocal.WireCommandType, timeout int64) (protocal.Reply, error) {
	var duration time.Duration
	if timeout <= 0 {
		duration = (time.Duration)(math.MaxInt64)
	} else {
		i := math.MaxInt64 / time.Second.Milliseconds()
		//overflow
		if i <= 0 {
			duration = (time.Duration)(math.MaxInt64)
		}
		duration = (time.Duration)(time.Second.Milliseconds() * timeout)
	}
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
