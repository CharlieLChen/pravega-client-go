package util

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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

			var a = 1
			ap := &a
			It("should be equal", func() {
				SetNil(ap)
				Expect(ap).To(BeNil())
			})
		})
	})
})
