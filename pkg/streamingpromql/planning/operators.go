// SPDX-License-Identifier: AGPL-3.0-only

package planning

import (
	"errors"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

var errSingleUseOperatorAlreadyUsed = errors.New("single use operator already used")

type OperatorFactory interface {
	Produce() (types.Operator, error)
}

// singleUseOperatorFactory is an OperatorFactory that returns an operator once, and
// fails if the operator is reused.
//
// Most nodes will use this OperatorFactory, with the exception of nodes that allow
// multiple parents to refer to them (eg. a common subexpression).
type singleUseOperatorFactory struct {
	used     bool
	operator types.Operator
}

func NewSingleUseOperatorFactory(operator types.Operator) OperatorFactory {
	return &singleUseOperatorFactory{
		operator: operator,
	}
}

func (f *singleUseOperatorFactory) Produce() (types.Operator, error) {
	if f.used {
		return nil, errSingleUseOperatorAlreadyUsed
	}

	f.used = true
	return f.operator, nil
}
