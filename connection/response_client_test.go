package connection

import (
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"io.pravega.pravega-client-go/errors"
	"io.pravega.pravega-client-go/protocol"
	"time"
)

var _ = Describe("ResponseClient Test", Label("responseClient"), func() {

	var (
		client      = NewResponseClient()
		requestId   = int64(100)
		writerId, _ = uuid.NewUUID()
		segmentName = "fakeSegment"
	)
	When("Offer reply", func() {
		appendSetup := protocol.NewAppendSetup(requestId, writerId, segmentName, 1)
		Context("with appendSetup", func() {
			go client.Offer(appendSetup)
			It("should get it successfully", func() {
				setup, err := client.GetAppendSetup(1000)
				Expect(err).To(BeNil())
				Expect(setup.GetRequestId()).To(Equal(requestId))
			})
		})

		Context("with appendSetup 1 second later", func() {
			go func() {
				time.Sleep(time.Second)
				client.Offer(appendSetup)
			}()
			It("should timeout", func() {
				setup, err := client.GetAppendSetup(10)
				Expect(err).To(Equal(errors.Error_Timeout))
				Expect(setup).To(BeNil())
			})
		})
	})
})
