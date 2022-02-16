package io

import "testing"

func TestByteBuffer_WriteAt(t *testing.T) {
	buffer := NewByteBuffer(100)
	data := []byte{1, 2, 3, 4}
	buffer.WriteAt(10, data)
	bytes, _ := buffer.ReadAt(10, 4)
	for i, _ := range bytes {
		if bytes[i] != data[i] {
			t.Fail()
		}
	}
}
