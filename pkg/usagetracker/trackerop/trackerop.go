// SPDX-License-Identifier: AGPL-3.0-only

// Package trackerop defines ring operations shared between the usage tracker
// server and client packages. It exists as a separate package to avoid an
// import cycle between usagetracker and usagetrackerclient.
package trackerop

import "github.com/grafana/dskit/ring"

var (
	// TrackSeriesOp is the ring operation used to locate instances for series tracking.
	TrackSeriesOp = ring.NewOp([]ring.InstanceState{ring.ACTIVE}, nil)
)
