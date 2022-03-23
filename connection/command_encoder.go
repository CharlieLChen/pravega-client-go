package connection

import (
	"io.pravega.pravega-client-go/io"
	"io.pravega.pravega-client-go/protocal"
)

const (
	EncoderBufferSize  = 1024 * 1024
	TypePlusLengthSize = 8
	TypeSize           = 4
	UnOccupied         = uint32(0)
	Occupied           = uint32(1)
	Failed             = uint32(10)
)

var (
	LengthPlaceholder = make([]byte, 4)
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
	encoder.Buffer.Reset()
}

func (encoder *CommandEncoder) EncodeCommand(command protocal.WireCommand) *io.ByteBuffer {
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
func (encoder *CommandEncoder) EncodeAppendBlock(appendBlock *protocal.AppendBlock) *io.ByteBuffer {
	encoder.Buffer.WriteInt32(appendBlock.GetType().Code)
	encoder.Buffer.Write(LengthPlaceholder)
	appendBlock.WriteFields(encoder.Buffer)
	return encoder.Buffer
}

func (encoder *CommandEncoder) EncodeEvent(event []byte) error {
	_, err := encoder.Buffer.Write(event)
	if err != nil {
		return err
	}
	return nil
}
