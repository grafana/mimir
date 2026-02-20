// SPDX-License-Identifier: AGPL-3.0-only

package labelaccess

import (
	"context"
	"errors"
)

type key int

const (
	contextKey key = 0
)

var (
	errNoMatcherSource = errors.New("no label matcher source")
)

type matcherSource interface {
	GetMatchers() LabelPolicySet
}

type labelMatcherCarrier struct {
	matcher LabelPolicySet
}

func (l *labelMatcherCarrier) GetMatchers() LabelPolicySet {
	return l.matcher
}

// ExtractLabelMatchersContext gets the embedded label matchers from the context.
func ExtractLabelMatchersContext(ctx context.Context) (LabelPolicySet, error) {
	source, ok := ctx.Value(contextKey).(matcherSource)

	if !ok {
		return nil, errNoMatcherSource
	}

	return source.GetMatchers(), nil
}

// InjectLabelMatchersContext returns a derived context containing the provided label matchers.
func InjectLabelMatchersContext(ctx context.Context, matchers LabelPolicySet) context.Context {
	return context.WithValue(ctx, interface{}(contextKey), &labelMatcherCarrier{matchers})
}
