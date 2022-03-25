package response

import (
	log "github.com/sirupsen/logrus"
	"io.pravega.pravega-client-go/protocol"
)

func Hello(hello *protocol.Hello) {
	if hello.LowVersion > protocol.WireVersion || hello.HighVersion < protocol.OldestCompatibleVersion {
		log.Errorf("Incompatible wire protocol versions %v", hello)
	} else {
		log.Infof("Received hello: %v", hello)
	}
}
