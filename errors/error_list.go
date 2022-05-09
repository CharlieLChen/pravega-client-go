package errors

import "fmt"

var (
	Error_Timeout                = fmt.Errorf("timeout")
	Error_All_Connections_Failed = fmt.Errorf("all connections failed")
)
