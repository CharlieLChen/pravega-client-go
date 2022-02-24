package protocal

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
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
func TestSetupConstructor_ReadFrom(t *testing.T) {
	newUUID, _ := uuid.NewUUID()
	setupAppend := NewSetupAppend(1, newUUID, "Hello 世界\n", "Hello 世界\n")
	buffer := &bytes.Buffer{}
	setupAppend.WriteFields(buffer)
	bytes := buffer.Bytes()
	fmt.Printf("%v", bytes)

}
