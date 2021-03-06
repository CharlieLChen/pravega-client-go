package protocol

import (
	"io.pravega.pravega-client-go/io"
)

const (
	EncoderBufferSize  = 1024 * 1024
	TypePlusLengthSize = 8
	TypeSize           = 4
)

var (
	LengthPlaceholder = make([]byte, 4)
	HelloBytes        = NewCommandEncoder().EncodeCommand(NewHello(10, 10)).Data()
)

type CommandEncoder struct {
	Buffer *io.ByteBuffer
}

func NewCommandEncoder() *CommandEncoder {
	return &CommandEncoder{
		Buffer: io.NewByteBuffer(EncoderBufferSize),
	}
}
func (encoder *CommandEncoder) Reset() {
	encoder.Buffer = io.NewByteBuffer(EncoderBufferSize)
}

func (encoder *CommandEncoder) ResetBuffer() {
	encoder.Buffer = io.NewByteBuffer(EncoderBufferSize)
}

func (encoder *CommandEncoder) EncodeCommand(command WireCommand) *io.ByteBuffer {
	startIdx := encoder.Buffer.Buffered()
	encoder.Buffer.WriteInt32(command.GetType().Code)
	encoder.Buffer.Write(LengthPlaceholder)
	command.WriteFields(encoder.Buffer)
	endIdx := encoder.Buffer.Buffered()
	fieldsSize := int32(endIdx - startIdx - TypePlusLengthSize)
	encoder.Buffer.WriteAt(startIdx+TypeSize, io.Int32toBytes(fieldsSize))
	return encoder.Buffer
}

func (encoder *CommandEncoder) WriteIntAt(position int, value int32) error {
	return encoder.Buffer.WriteAt(position, io.Int32toBytes(value))
}
func (encoder *CommandEncoder) EncodeAppendBlock(appendBlock *AppendBlock) *io.ByteBuffer {
	encoder.Buffer.WriteInt32(appendBlock.GetType().Code)
	encoder.Buffer.Write(LengthPlaceholder)
	appendBlock.WriteFields(encoder.Buffer)
	return encoder.Buffer
}
