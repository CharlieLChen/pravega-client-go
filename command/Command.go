package command

type Command struct {
	Code    int
	Message interface{}
}

const (
	Flush  = 1
	Resend = 2
	Done   = 3
)
