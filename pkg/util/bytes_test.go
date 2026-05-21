// SPDX-License-Identifier: AGPL-3.0-only

package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFormatIBytes(t *testing.T) {
	tests := map[uint64]string{
		0:           "0 B",
		1:           "1 B",
		9:           "9 B",
		10:          "10 B",
		1023:        "1023 B",
		1024:        "1.0 KiB",
		1536:        "1.5 KiB",
		82854982:    "79 MiB",
		1024 * 1024: "1.0 MiB",
		1 << 30:     "1.0 GiB",
		1 << 40:     "1.0 TiB",
		1 << 50:     "1.0 PiB",
		1 << 60:     "1.0 EiB",
	}
	for in, want := range tests {
		assert.Equal(t, want, FormatIBytes(in), "FormatIBytes(%d)", in)
	}
}
