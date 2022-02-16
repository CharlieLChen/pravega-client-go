package connection

import (
	"io.pravega.pravega-client-go/io"
	"io.pravega.pravega-client-go/protocal"
)

const (
	EncoderBufferSize  = 1024 * 1024
	TypePlusLengthSize = 8
	TypeSize           = 4
)

var (
	LengthPlaceholder = make([]byte, 4)
)

type CommandEncoder struct {
	buffer *io.ByteBuffer
}

func NewCommandEncoder() *CommandEncoder {
	return &CommandEncoder{
		buffer: io.NewByteBuffer(EncoderBufferSize),
	}
}
func WriteMessage(msg protocal.WireCommand, destination *io.ByteBuffer) {
	/***
	  int startIdx = destination.writerIndex();
	  ByteBufOutputStream bout = new ByteBufOutputStream(destination);
	  bout.writeInt(msg.getType().getCode());
	  bout.write(LENGTH_PLACEHOLDER);
	  msg.writeFields(bout);
	  bout.flush();
	  bout.close();
	  int endIdx = destination.writerIndex();
	  int fieldsSize = endIdx - startIdx - TYPE_PLUS_LENGTH_SIZE;
	  destination.setInt(startIdx + TYPE_SIZE, fieldsSize);
	  return endIdx - startIdx;
	*/
	startIdx := destination.Buffered()
	destination.WriteInt32(msg.GetType().Code)
	destination.Write(LengthPlaceholder)
	msg.WriteFields(destination)
	endIdx := destination.Buffered()
	fieldsSize := int32(endIdx - startIdx - TypePlusLengthSize)

	destination.WriteAt(startIdx+TypeSize, io.Int32toBytes(fieldsSize))

}
func (encoder *CommandEncoder) Write(command protocal.WireCommand) *io.ByteBuffer {
	WriteMessage(command, encoder.buffer)
	return encoder.buffer

}
