package protocal

import (
	"bytes"
	"testing"
)

func TestHello_ReadAndWrite(t *testing.T) {
	hello := NewHello(-45, 50)
	buffer := &bytes.Buffer{}
	hello.WriteFields(buffer)
	res, _ := (hello.GetType().Factory.ReadFrom(buffer, 0))
	result := res.(*Hello)
	if hello.HighVersion != result.HighVersion || hello.LowVersion != result.LowVersion {
		t.Fail()
	}
}
