package util

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/spaolacci/murmur3"
	"golang.org/x/text/encoding/unicode"
	"io.pravega.pravega-client-go/io"
	"log"
)

var (
	mask        = int64(0x000fffffffffffff)
	LeadingBits = int64(0x3ff0000000000000)
)

var _ = Describe("Future Util Test", func() {
	When("Future Util Test", func() {
		Context("Get And Complete 1", func() {
			future := NewFuture()

			future.Complete(1)
			get := future.Get()
			It("should be equal", func() {
				result, ok := get.(int)
				Expect(ok).To(BeTrue())
				Expect(result).To(Equal(1))
			})
		})
	})

	When("Set Nil Test", func() {
		Context("Get And Complete 1", func() {

			It("should be equal", func() {
				hasher := murmur3.New128WithSeed(1741865571)
				data := []byte("hello")
				log.Print(len(data))
				encoder := unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM).NewEncoder()
				encoded, _ := encoder.Bytes(data)
				log.Print(len(encoded))
				hasher.Write(encoded)
				upper, lower := hasher.Sum128()
				log.Print(upper)
				log.Print(lower)
				log.Print(io.Int64ToFloat64(upper))
			})
		})
	})
})
