// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/matchers.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package util

import (
	"strings"

	"github.com/prometheus/prometheus/model/labels"
)

// MultiMatchersStringer implements Stringer for a slice of slices of Prometheus matchers. Useful for logging.
type MultiMatchersStringer [][]*labels.Matcher

func (s MultiMatchersStringer) String() string {
	var b strings.Builder
	for _, multi := range s {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('{')
		b.WriteString(MatchersStringer(multi).String())
		b.WriteByte('}')
	}

	return b.String()
}

// MatchersStringer implements Stringer for a slice of Prometheus matchers. Useful for logging.
type MatchersStringer []*labels.Matcher

func (s MatchersStringer) String() string {
	var b strings.Builder
	for _, m := range s {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(m.String())
	}

	return b.String()
}
