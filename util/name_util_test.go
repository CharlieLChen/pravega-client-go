package util

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	v1 "io.pravega.pravega-client-go/controller/proto"
)

var _ = Describe("Name Util Test", func() {
	When("Get Segment Name", func() {
		Context("", func() {
			segmentId := &v1.SegmentId{
				StreamInfo: &v1.StreamInfo{
					Scope:  "test",
					Stream: "test",
				},
				SegmentId: 0,
			}
			segmentName := GetQualifiedStreamSegmentName(segmentId)
			id, _ := SegmentNameToId(segmentName)
			
			It("should be equal", func() {
				Expect(id.SegmentId).To(Equal(segmentId.SegmentId))
				Expect(id.StreamInfo.Stream).To(Equal(segmentId.StreamInfo.Stream))
				Expect(id.StreamInfo.Scope).To(Equal(segmentId.StreamInfo.Scope))
			})
		})
	})
})
