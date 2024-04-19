// SPDX-License-Identifier: AGPL-3.0-only

package usagestats

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCounter(t *testing.T) {
	c := GetCounter("test_counter")
	c.Inc(100)
	c.Inc(200)
	c.Inc(300)
	time.Sleep(1 * time.Second)
	c.updateRate()
	v := c.Value()
	require.Equal(t, int64(600), v["total"])
	require.GreaterOrEqual(t, v["rate"], float64(590))
	c.reset()
	require.Equal(t, int64(0), c.Value()["total"])
	require.Equal(t, float64(0), c.Value()["rate"])
}
