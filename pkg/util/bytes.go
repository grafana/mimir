// SPDX-License-Identifier: AGPL-3.0-only

package util

import (
	"fmt"
	"math"
)

// FormatIBytes returns a human-readable representation of an IEC (1024-based) size,
// matching the output format of github.com/dustin/go-humanize's IBytes (e.g. "79 MiB").
func FormatIBytes(s uint64) string {
	const base = 1024
	if s < 10 {
		return fmt.Sprintf("%d B", s)
	}
	sizes := []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}
	e := int(math.Floor(math.Log(float64(s)) / math.Log(base)))
	val := math.Floor(float64(s)/math.Pow(base, float64(e))*10+0.5) / 10
	format := "%.0f %s"
	if val < 10 {
		format = "%.1f %s"
	}
	return fmt.Sprintf(format, val, sizes[e])
}
