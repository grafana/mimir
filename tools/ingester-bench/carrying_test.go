// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Carrying per-series metadata makes records bigger, and a profile whose series send none carries nothing.
func TestCarryingCost(t *testing.T) {
	profile, err := loadProfile("native-metadata/threshold.json")
	require.NoError(t, err)
	c, err := carryingCost(profile, 100_000, 10)
	require.NoError(t, err)
	require.Greater(t, c.WithoutBytesPerSample, 0.0)
	require.Greater(t, c.Increase, 0.0)

	profile.Series.MetadataShare = 0
	c, err = carryingCost(profile, 100_000, 10)
	require.NoError(t, err)
	require.Equal(t, c.WithBytesPerSample, c.WithoutBytesPerSample)
}
