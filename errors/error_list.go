package errors

import "fmt"

var (
	Error_Timeout                = fmt.Errorf("timeout")
	Error_UnexpectedType         = fmt.Errorf("unexpectedtype")
	Error_Failure                = fmt.Errorf("failure")
	Error_All_Connections_Failed = fmt.Errorf("all connections failed")
)
