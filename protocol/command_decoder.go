package protocol

import (
	"io"
	io_util "io.pravega.pravega-client-go/io"
)

func Decode(reader io.Reader) (WireCommand, error) {
	typeBytes := make([]byte, 4)
	lengthBytes := make([]byte, 4)

	_, err := reader.Read(typeBytes)
	if err != nil {
		return nil, err
	}
	_, err = reader.Read(lengthBytes)
	if err != nil {
		return nil, err
	}
	types := io_util.BytestoInt32(typeBytes)
	length := io_util.BytestoInt32(lengthBytes)

	commandType := TypesMapping[types]
	return commandType.Factory.ReadFrom(reader, length)
}
