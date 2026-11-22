// SPDX-License-Identifier: AGPL-3.0-only

package grpctee

import (
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

// comparePair is a minimal placeholder for the real comparison.
// It logs when the two frames of a pair differ in length, or when one side has no frame.
func comparePair(logger log.Logger, fullMethod string, pair ResponseFramePair) {
	switch {
	case pair.primary == nil:
		level.Warn(logger).Log("msg", "secondary frame has no primary frame", "method", fullMethod, "secondary_bytes", len(pair.secondary))
	case pair.secondary == nil:
		level.Warn(logger).Log("msg", "primary frame has no secondary frame", "method", fullMethod, "primary_bytes", len(pair.primary))
	case len(pair.primary) != len(pair.secondary):
		level.Warn(logger).Log("msg", "frame length mismatch", "method", fullMethod, "primary_bytes", len(pair.primary), "secondary_bytes", len(pair.secondary))
	}
}
