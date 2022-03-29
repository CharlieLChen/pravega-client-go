package io

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"io"
)

type ByteBuffer struct {
	buffer *bytes.Buffer
}

func NewByteBuffer(size int) *ByteBuffer {
	slice := make([]byte, size)
	buffer := bytes.NewBuffer(slice)
	buffer.Reset()
	return &ByteBuffer{
		buffer: buffer,
	}
}
func (bb *ByteBuffer) Reset() {
	bb.buffer.Reset()
}

func (bb *ByteBuffer) WriteInt32(data int32) error {
	_, err := bb.buffer.Write(Int32toBytes(data))
	return err
}

func (bb *ByteBuffer) WriteInt64(data int64) error {
	_, err := bb.buffer.Write(Int64toBytes(data))
	return err
}

func (bb *ByteBuffer) WriteBool(value bool) error {
	_, err := bb.buffer.Write(BooltoBytes(value))
	return err
}

func (bb *ByteBuffer) ReadUTF(in io.Reader) (string, error) {
	return ReadUTF(in)
}

func (bb *ByteBuffer) WriteUTF(data string) error {
	return WriteUTF(bb.buffer, data)
}

func (bb *ByteBuffer) WriteUUid(id uuid.UUID) error {
	binary, err := id.MarshalBinary()
	if err != nil {
		return err
	}
	return bb.Write(binary)
}

func (bb *ByteBuffer) Write(data []byte) error {
	_, err := bb.buffer.Write(data)
	return err
}
func (bb *ByteBuffer) Buffered() int {
	return len(bb.buffer.Bytes())
}
func (bb *ByteBuffer) Data() []byte {
	return bb.buffer.Bytes()
}

func (bb *ByteBuffer) WriteAt(startAt int, data []byte) error {
	dataLen := len(data)
	if dataLen+startAt > bb.buffer.Cap() {
		return fmt.Errorf("write out of Index")
	}
	for i, datum := range data {
		bb.buffer.Bytes()[i+startAt] = datum
	}
	return nil
}
func (bb *ByteBuffer) ReadAt(startAt int, length int) ([]byte, error) {
	if length+startAt > bb.buffer.Cap() {
		return nil, fmt.Errorf("read out of Index")
	}
	data := make([]byte, length)
	for i := 0; i < length; i++ {
		data[i] = bb.buffer.Bytes()[startAt+i]
	}
	return data, nil
}
