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
		host                      = "localhost"
		callStack                 = "callStack"
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

	When("KeepAlive WireCommand", func() {
		Context("encode and decode", func() {
			keepAlive := NewKeepAlive()
			encoder := NewCommandEncoder()
			encoder.EncodeCommand(keepAlive)
			buffer := bytes.NewBuffer(encoder.Buffer.Data())
			decoded, err := Decode(buffer)

			It("should be equal", func() {
				Expect(err).To(BeNil())
				Expect(decoded.GetType()).To(Equal(keepAlive.GetType()))

				val := decoded.(*KeepAlive)
				Expect(val.IsFailure()).To(BeFalse())
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
	When("WrongHost WireCommand", func() {
		Context("encode and decode", func() {
			wrongHost := NewWrongHost(requestId, segmentName, host, callStack)
			encoder := NewCommandEncoder()
			encoder.EncodeCommand(wrongHost)
			buffer := bytes.NewBuffer(encoder.Buffer.Data())
			decoded, err := Decode(buffer)

			It("should be equal", func() {
				Expect(err).To(BeNil())
				Expect(decoded.GetType()).To(Equal(wrongHost.GetType()))

				val := decoded.(*WrongHost)
				Expect(val.Segment).To(Equal(val.Segment))
				Expect(len(val.ServerStackTrace)).To(Equal(len(val.ServerStackTrace)))
				Expect(val.IsFailure()).To(BeTrue())
			})
		})
	})

	When("SegmentSealed WireCommand", func() {
		Context("encode and decode", func() {
			segmentSealed := NewSegmentSealed(requestId, segmentName, host, offset)
			encoder := NewCommandEncoder()
			encoder.EncodeCommand(segmentSealed)
			buffer := bytes.NewBuffer(encoder.Buffer.Data())
			decoded, err := Decode(buffer)

			It("should be equal", func() {
				Expect(err).To(BeNil())
				Expect(decoded.GetType()).To(Equal(segmentSealed.GetType()))

				val := decoded.(*SegmentSealed)
				Expect(val.Segment).To(Equal(val.Segment))
				Expect(len(val.ServerStackTrace)).To(Equal(len(val.ServerStackTrace)))
				Expect(val.IsFailure()).To(BeTrue())
			})
		})
	})

	When("SegmentExists WireCommand", func() {
		Context("encode and decode", func() {
			segmentExists := NewSegmentExists(requestId, segmentName, host)
			encoder := NewCommandEncoder()
			encoder.EncodeCommand(segmentExists)
			buffer := bytes.NewBuffer(encoder.Buffer.Data())
			decoded, err := Decode(buffer)

			It("should be equal", func() {
				Expect(err).To(BeNil())
				Expect(decoded.GetType()).To(Equal(segmentExists.GetType()))

				val := decoded.(*SegmentExists)
				Expect(val.Segment).To(Equal(val.Segment))
				Expect(len(val.ServerStackTrace)).To(Equal(len(val.ServerStackTrace)))
				Expect(val.IsFailure()).To(BeTrue())
			})
		})
	})

	When("NoSuchSegment WireCommand", func() {
		Context("encode and decode", func() {
			noSuchSegment := NewNoSuchSegment(requestId, segmentName, callStack, offset)
			encoder := NewCommandEncoder()
			encoder.EncodeCommand(noSuchSegment)
			buffer := bytes.NewBuffer(encoder.Buffer.Data())
			decoded, err := Decode(buffer)

			It("should be equal", func() {
				Expect(err).To(BeNil())
				Expect(decoded.GetType()).To(Equal(noSuchSegment.GetType()))

				val := decoded.(*NoSuchSegment)
				Expect(val.Segment).To(Equal(val.Segment))
				Expect(len(val.ServerStackTrace)).To(Equal(len(val.ServerStackTrace)))
				Expect(val.IsFailure()).To(BeTrue())
			})
		})
	})

	When("InvalidEventNumber WireCommand", func() {
		Context("encode and decode", func() {
			invalidEventNumber := NewInvalidEventNumber(writerId, eventNumber, callStack)
			encoder := NewCommandEncoder()
			encoder.EncodeCommand(invalidEventNumber)
			buffer := bytes.NewBuffer(encoder.Buffer.Data())
			decoded, err := Decode(buffer)

			It("should be equal", func() {
				Expect(err).To(BeNil())
				Expect(decoded.GetType()).To(Equal(invalidEventNumber.GetType()))

				val := decoded.(*InvalidEventNumber)
				Expect(len(val.ServerStackTrace)).To(Equal(len(val.ServerStackTrace)))
				Expect(val.EventNumber).To(Equal(val.EventNumber))
				Expect(val.IsFailure()).To(BeTrue())
			})
		})
	})

	When("SegmentTruncated WireCommand", func() {
		Context("encode and decode", func() {
			segmentTruncated := NewSegmentTruncated(requestId, segmentName)
			encoder := NewCommandEncoder()
			encoder.EncodeCommand(segmentTruncated)
			buffer := bytes.NewBuffer(encoder.Buffer.Data())
			decoded, err := Decode(buffer)

			It("should be equal", func() {
				Expect(err).To(BeNil())
				Expect(decoded.GetType()).To(Equal(segmentTruncated.GetType()))

				val := decoded.(*SegmentTruncated)
				Expect(val.Segment).To(Equal(val.Segment))
				Expect(val.IsFailure()).To(BeTrue())
			})
		})
	})

	When("OperationUnsupported WireCommand", func() {
		Context("encode and decode", func() {
			operationUnsupported := NewOperationUnsupported(requestId, segmentName, callStack)
			encoder := NewCommandEncoder()
			encoder.EncodeCommand(operationUnsupported)
			buffer := bytes.NewBuffer(encoder.Buffer.Data())
			decoded, err := Decode(buffer)

			It("should be equal", func() {
				Expect(err).To(BeNil())
				Expect(decoded.GetType()).To(Equal(operationUnsupported.GetType()))

				val := decoded.(*OperationUnsupported)
				Expect(val.OperationName).To(Equal(val.OperationName))
				Expect(val.IsFailure()).To(BeTrue())
			})
		})
	})

})
