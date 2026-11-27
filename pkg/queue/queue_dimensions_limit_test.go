// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDimensionInflightLimit(t *testing.T) {
	t.Run("a ratio of 0 is never at the limit", func(t *testing.T) {
		var l dimensionInflightLimit
		l.inc()
		require.False(t, l.atLimit(0))
		require.False(t, l.atLimit(4))
	})

	t.Run("the limit is the ratio of the connected consumer-workers", func(t *testing.T) {
		l := dimensionInflightLimit{maxRatio: 0.5}
		l.inc()
		require.False(t, l.atLimit(4))
		l.inc()
		require.True(t, l.atLimit(4))
		require.False(t, l.atLimit(6))
	})

	t.Run("the limit is at least 1", func(t *testing.T) {
		l := dimensionInflightLimit{maxRatio: 0.1}
		require.False(t, l.atLimit(0))
		l.inc()
		require.True(t, l.atLimit(0))
		require.True(t, l.atLimit(1))
	})

	t.Run("a dec never goes below 0", func(t *testing.T) {
		l := dimensionInflightLimit{maxRatio: 0.5}
		l.inc()
		l.dec()
		l.dec()
		require.Equal(t, 0, l.inflight)
	})
}
