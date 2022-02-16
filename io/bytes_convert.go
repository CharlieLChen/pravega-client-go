package io

import (
	"bytes"
	"encoding/binary"
	"io"
)

func Write(out io.Writer, data interface{}) error {
	return binary.Write(out, binary.BigEndian, data)
}
func Read(in io.Reader, data interface{}) error {
	return binary.Read(in, binary.BigEndian, data)
}

func Int32toBytes(value int32) []byte {
	bs := make([]byte, 0)
	buffer := bytes.NewBuffer(bs)
	Write(buffer, value)
	return buffer.Bytes()
}
func BytestoInt32(bs []byte) int32 {
	buffer := bytes.NewBuffer(bs)
	var res int32
	Read(buffer, &res)
	return res
}
