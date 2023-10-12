package internal

import (
	"github.com/failsafe-go/failsafe-go/common"
)

func FailureResult[R any](err error) *common.PolicyResult[R] {
	return &common.PolicyResult[R]{
		Error: err,
		Done:  true,
	}
}
