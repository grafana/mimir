// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/pool"
)

func TestMaxExpectedSeriesPerResultConstantIsPowerOfTwo(t *testing.T) {
	// Although not strictly required (as the code should handle MaxExpectedSeriesPerResult not being a power of two correctly),
	// it is best that we keep it as one for now.
	require.True(t, pool.IsPowerOfTwo(MaxExpectedSeriesPerResult), "MaxExpectedSeriesPerResult must be a power of two")
}
