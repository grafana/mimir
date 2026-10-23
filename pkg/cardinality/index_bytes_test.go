// SPDX-License-Identifier: AGPL-3.0-only

package cardinality

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBoundMaxIndexBytes(t *testing.T) {
	for name, tc := range map[string]struct {
		asked, limit, want int64
	}{
		"no limit keeps an unbounded request": {asked: 0, limit: 0, want: 0},
		"no limit keeps the request's bound":  {asked: 10, limit: 0, want: 10},
		"an unbounded request gets the limit": {asked: 0, limit: 100, want: 100},
		"a smaller request is kept":           {asked: 10, limit: 100, want: 10},
		"a larger request is cut":             {asked: 1000, limit: 100, want: 100},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, BoundMaxIndexBytes(tc.asked, tc.limit))
		})
	}
}
