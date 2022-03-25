package connection

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"testing"
)

func TestConnections(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Connections Suite")
}
