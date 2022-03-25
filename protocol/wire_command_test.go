package protocol

import (
	"bytes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Protocol Test", Label("Protocol"), func() {

	When("Hello WireCommand", func() {
		Context("encode and decode", func() {
			hello := NewHello(100, 20)
			encoder := NewCommandEncoder()
			encoder.EncodeCommand(hello)
			buffer := bytes.NewBuffer(encoder.Buffer.Data())
			decode, err := Decode(buffer)
			It("should be equal", func() {
				Expect(err).To(BeNil())
				Expect(decode.GetType()).To(Equal(hello.GetType()))
				h := decode.(*Hello)
				Expect(hello.HighVersion).To(Equal(h.HighVersion))
				Expect(hello.LowVersion).To(Equal(h.LowVersion))
			})
		})
	})

	When("Hello WireCommand", func() {
		Context("encode and decode", func() {
			hello := NewHello(100, 20)
			encoder := NewCommandEncoder()
			encoder.EncodeCommand(hello)
			buffer := bytes.NewBuffer(encoder.Buffer.Data())
			decode, err := Decode(buffer)
			It("should be equal", func() {
				Expect(err).To(BeNil())
				Expect(decode.GetType()).To(Equal(hello.GetType()))
				h := decode.(*Hello)
				Expect(hello.HighVersion).To(Equal(h.HighVersion))
				Expect(hello.LowVersion).To(Equal(h.LowVersion))
			})
		})
	})
})
