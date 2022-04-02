package command

type Command struct {
	Code    int
	Message interface{}
}

const (
	Flush = 1
	Stop  = 2
	Done  = 3
)
