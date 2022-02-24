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

func ReadUTF(in io.Reader) (string, error) {
	var length int16
	err := Read(in, &length)
	if err != nil {
		return "", err
	}
	buffer, err := ReadWithLength(in, int(length))
	return string(buffer), nil

}

func WriteUTF(out io.Writer, data string) error {
	normalUtf8 := []byte(data)
	length := make([]byte, 2)
	bodyLength := len(normalUtf8)
	length[0] = byte(bodyLength >> 8 & 0xFF)
	length[1] = byte(bodyLength >> 0 & 0xFF)
	modifiedUTF8 := append(length, normalUtf8...)
	_, err := out.Write(modifiedUTF8)
	if err != nil {
		return err
	}
	return nil
}

func ReadWithLength(in io.Reader, size int) ([]byte, error) {
	buffer := make([]byte, size)
	_, err := in.Read(buffer)
	if err != nil {
		return nil, err
	}
	return buffer, nil
}
