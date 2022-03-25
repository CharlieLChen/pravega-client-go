package io

import (
	"encoding/binary"
	"github.com/google/uuid"
	"io"
)

func Write(out io.Writer, data interface{}) error {
	return binary.Write(out, binary.BigEndian, data)
}
func Read(in io.Reader, data interface{}) error {
	return binary.Read(in, binary.BigEndian, data)
}

func Int32toBytes(value int32) []byte {
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, uint32(value))
	return bs
}

func BytestoInt32(bs []byte) int32 {
	value := binary.BigEndian.Uint32(bs)
	return int32(value)
}

func BytestoInt64(bs []byte) int64 {
	value := binary.BigEndian.Uint64(bs)
	return int64(value)
}

func Int64toBytes(value int64) []byte {
	bs := make([]byte, 8)
	binary.BigEndian.PutUint64(bs, uint64(value))
	return bs
}

func ReadInt32(in io.Reader) (int32, error) {
	var value int32
	err := Read(in, &value)
	if err != nil {
		return 0, err
	}
	return value, nil
}

func ReadInt64(in io.Reader) (int64, error) {
	var value int64
	err := Read(in, &value)
	if err != nil {
		return 0, err
	}
	return value, nil
}

func ReadUUid(in io.Reader) (*uuid.UUID, error) {
	bytes, err := ReadWithLength(in, 16)
	if err != nil {
		return nil, err
	}
	writerId, err := uuid.FromBytes(bytes)
	if err != nil {
		return nil, err
	}
	return &writerId, nil
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
