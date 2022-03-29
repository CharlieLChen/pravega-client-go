package protocol

import (
	"bytes"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	v1 "io.pravega.pravega-client-go/controller/proto"
	"io.pravega.pravega-client-go/util"
)

var _ = Describe("Protocol Test", Label("Protocol"), func() {

	var (
		requestId       = int64(100)
		lastEventNumber = int64(1)
		writerId, _     = uuid.NewUUID()
		segmentId       = &v1.SegmentId{
			StreamInfo: &v1.StreamInfo{
				Scope:  "test",
				Stream: "test",
			},
			SegmentId: 0,
		}
		segmentName       = util.GetQualifiedStreamSegmentName(segmentId)
		token             = ""
		sizeOfWholeEvents = int32(1)
		numEvents         = int32(1)
		eventNumber       = int64(1)
		expectedOffset    = int64(1)
		data              = []byte("hello world")
		event             = &Event{
			Data: data,
		}
		previousEventNumber       = int64(1)
		currentSegmentWriteOffset = int64(1)
		offset                    = int64(0)
		suggestedLength           = int64(100)
	)
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

	When("SetupAppend WireCommand", func() {
		Context("encode and decode", func() {
			setup := NewSetupAppend(requestId, writerId, segmentName, token)
			encoder := NewCommandEncoder()
			encoder.EncodeCommand(setup)
			buffer := bytes.NewBuffer(encoder.Buffer.Data())
			decoded, err := Decode(buffer)
			It("should be equal", func() {
				Expect(err).To(BeNil())
				Expect(decoded.GetType()).To(Equal(setup.GetType()))

				s := decoded.(*SetupAppend)
				Expect(s.Segment).To(Equal(setup.Segment))
			})
		})
	})

	When("AppendSetup WireCommand", func() {
		Context("encode and decode", func() {
			appendSetup := NewAppendSetup(requestId, writerId, segmentName, lastEventNumber)
			encoder := NewCommandEncoder()
			encoder.EncodeCommand(appendSetup)
			buffer := bytes.NewBuffer(encoder.Buffer.Data())
			decoded, err := Decode(buffer)
			It("should be equal", func() {
				Expect(err).To(BeNil())
				Expect(decoded.GetType()).To(Equal(appendSetup.GetType()))

				val := decoded.(*AppendSetup)
				Expect(val.Segment).To(Equal(val.Segment))
			})
		})
	})

	When("AppendBlock WireCommand", func() {
		Context("encode and decode", func() {
			appendBlock := NewAppendBlock(writerId)
			encoder := NewCommandEncoder()
			encoder.EncodeCommand(appendBlock)
			buffer := bytes.NewBuffer(encoder.Buffer.Data())
			decoded, err := Decode(buffer)
			It("should be equal", func() {
				Expect(err).To(BeNil())
				Expect(decoded.GetType()).To(Equal(appendBlock.GetType()))

				val := decoded.(*AppendBlock)
				Expect(val.WriterId).To(Equal(val.WriterId))
			})
		})
	})

	When("AppendBlockEnd WireCommand", func() {
		Context("encode and decode", func() {
			appendBlockEnd := NewAppendBlockEnd(writerId, sizeOfWholeEvents, numEvents, lastEventNumber, requestId)
			encoder := NewCommandEncoder()
			encoder.EncodeCommand(appendBlockEnd)
			buffer := bytes.NewBuffer(encoder.Buffer.Data())
			decoded, err := Decode(buffer)
			It("should be equal", func() {
				Expect(err).To(BeNil())
				Expect(decoded.GetType()).To(Equal(appendBlockEnd.GetType()))

				val := decoded.(*AppendBlockEnd)
				Expect(val.WriterId).To(Equal(val.WriterId))
			})
		})
	})

	When("ConditionalAppend WireCommand", func() {
		Context("encode and decode", func() {
			conditionalAppend := NewConditionalAppend(requestId, writerId, eventNumber, expectedOffset, event)
			encoder := NewCommandEncoder()
			encoder.EncodeCommand(conditionalAppend)
			buffer := bytes.NewBuffer(encoder.Buffer.Data())
			decoded, err := Decode(buffer)

			It("should be equal", func() {
				Expect(err).To(BeNil())
				Expect(decoded.GetType()).To(Equal(conditionalAppend.GetType()))

				val := decoded.(*ConditionalAppend)
				Expect(val.WriterId).To(Equal(val.WriterId))
				Expect(len(val.Event.Data)).To(Equal(len(conditionalAppend.Event.Data)))

			})
		})
	})

	When("DataAppended WireCommand", func() {
		Context("encode and decode", func() {
			dataAppended := NewDataAppended(writerId, requestId, eventNumber, previousEventNumber, currentSegmentWriteOffset)
			encoder := NewCommandEncoder()
			encoder.EncodeCommand(dataAppended)
			buffer := bytes.NewBuffer(encoder.Buffer.Data())
			decoded, err := Decode(buffer)

			It("should be equal", func() {
				Expect(err).To(BeNil())
				Expect(decoded.GetType()).To(Equal(dataAppended.GetType()))

				val := decoded.(*DataAppended)
				Expect(val.WriterId).To(Equal(val.WriterId))

			})
		})
	})

	When("ConditionalCheckFailed WireCommand", func() {
		Context("encode and decode", func() {
			conditionalCheckFailed := NewConditionalCheckFailed(writerId, requestId, eventNumber)
			encoder := NewCommandEncoder()
			encoder.EncodeCommand(conditionalCheckFailed)
			buffer := bytes.NewBuffer(encoder.Buffer.Data())
			decoded, err := Decode(buffer)

			It("should be equal", func() {
				Expect(err).To(BeNil())
				Expect(decoded.GetType()).To(Equal(conditionalCheckFailed.GetType()))

				val := decoded.(*ConditionalCheckFailed)
				Expect(val.WriterId).To(Equal(val.WriterId))

			})
		})
	})

	When("ReadSegment WireCommand", func() {
		Context("encode and decode", func() {
			readSegment := NewReadSegment(segmentName, token, requestId, offset, suggestedLength)
			encoder := NewCommandEncoder()
			encoder.EncodeCommand(readSegment)
			buffer := bytes.NewBuffer(encoder.Buffer.Data())
			decoded, err := Decode(buffer)

			It("should be equal", func() {
				Expect(err).To(BeNil())
				Expect(decoded.GetType()).To(Equal(readSegment.GetType()))

				val := decoded.(*ReadSegment)
				Expect(val.Segment).To(Equal(val.Segment))

			})
		})
	})

	When("SegmentRead WireCommand", func() {
		Context("encode and decode", func() {
			segmentRead := NewSegmentRead(segmentName, requestId, offset, false, false, data)
			encoder := NewCommandEncoder()
			encoder.EncodeCommand(segmentRead)
			buffer := bytes.NewBuffer(encoder.Buffer.Data())
			decoded, err := Decode(buffer)

			It("should be equal", func() {
				Expect(err).To(BeNil())
				Expect(decoded.GetType()).To(Equal(segmentRead.GetType()))

				val := decoded.(*SegmentRead)
				Expect(val.Segment).To(Equal(val.Segment))
				Expect(len(val.Data)).To(Equal(len(val.Data)))
			})
		})
	})
})
