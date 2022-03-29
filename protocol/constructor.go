package protocol

import (
	"io"
)

type Constructor interface {
	ReadFrom(reader io.Reader, length int32) (WireCommand, error)
}
