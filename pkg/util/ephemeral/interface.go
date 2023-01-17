// SPDX-License-Identifier: AGPL-3.0-only

package ephemeral

import "github.com/grafana/mimir/pkg/mimirpb"

type SeriesCheckerByUser interface {
	// EphemeralChecker returns an object which checks a series and decides whether
	// this series should be ephemeral based on the given user's configuration.
	// Can return nil if the user has no relevant configuration.
	EphemeralChecker(user string, source mimirpb.WriteRequest_SourceEnum) SeriesChecker
}

type SeriesChecker interface {
	// ShouldMarkEphemeral checks if a series with the given labelset should be marked as ephemeral.
	ShouldMarkEphemeral(labelSet []mimirpb.LabelAdapter) bool
}
