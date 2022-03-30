package connection

import (
	. "github.com/onsi/ginkgo/v2"
	log "github.com/sirupsen/logrus"
	"io.pravega.pravega-client-go/fake"
	"io.pravega.pravega-client-go/protocol"
	"os"
)

var (
	server     = fake.FakeServer{}
	dispatcher = NewResponseDispatcher()
	pool       = NewConnectionPool(dispatcher, 5)
	sockts     = NewSockets(dispatcher, fake.NewFakeController())
)
var _ = BeforeSuite(func() {
	response := make(chan protocol.Reply)
	go func() {
		for reply := range response {
			log.Debugf("Received the response %v", reply)
		}
	}()
	dispatcher.Register(int64(10), response)
	err := server.CreateFakeServer()
	if err != nil {
		log.Fatal("Failed to create fakeServer for test")
		os.Exit(1)
	}

})
var _ = AfterSuite(func() {
	for _, connection := range pool.connections {
		connection.Close()
	}
	for _, connectionPool := range sockts.pools {
		connectionPool.Close()
	}
	server.Close()
})

//var _ = Describe("Connection Pool Test", Label("Connection Pool"), func() {
//
//	When("Create Connection", func() {
//		Context("less than maxConnectionPerHost", func() {
//			It("should success", func() {
//				server.WriteHello()
//				connection, err := pool.createConnection(fake.Host)
//				Expect(err).To(BeNil())
//				Expect(connection.index).To(Equal(0))
//				Expect(connection.state).To(Equal(UnOccupied))
//			})
//		})
//	})
//
//	When("Get Connection", func() {
//		Context("less than maxConnectionPerHost", func() {
//			It("should success", func() {
//				connection, err := pool.getConnection(fake.Host)
//				Expect(err).To(BeNil())
//				Expect(connection.index).To(Equal(0))
//				Expect(connection.state).To(Equal(Occupied))
//			})
//		})
//	})
//	When("Create Connection", func() {
//		Context("more than maxConnectionPerHost", func() {
//			It("has maxConnection 5", func() {
//				for i := 0; i < 10; i++ {
//					server.WriteHello()
//					pool.createConnection(fake.Host)
//				}
//				Expect(len(pool.connections)).To(Equal(5))
//
//			})
//		})
//	})
//
//	When("Connections ", func() {
//		Context("are all failed", func() {
//			It("should fail", func() {
//				for _, connection := range pool.connections {
//					connection.releaseWithFailure()
//				}
//				connection, err := pool.getConnection(fake.Host)
//				Expect(err).To(Equal(errors.Error_All_Connections_Failed))
//				Expect(connection).To(BeNil())
//			})
//		})
//	})
//
//	When("Test Sockets", func() {
//		Context("Test Sockets", func() {
//			It("should success", func() {
//				server.WriteAppendSetup()
//				connection, err := sockts.getConnection(fake.Host)
//				time.Sleep(time.Second)
//				Expect(err).To(BeNil())
//				connection.release()
//			})
//		})
//	})
//
//})
