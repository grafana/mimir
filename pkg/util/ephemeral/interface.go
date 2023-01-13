// SPDX-License-Identifier: AGPL-3.0-only

package ephemeral

import "github.com/grafana/mimir/pkg/mimirpb"

type SeriesCheckerByUser interface {
	// EphemeralChecker returns an object which checks a series and decides whether
	// this series should be ephemeral based on the given user's configuration.
	EphemeralChecker(string) SeriesChecker
}

type SeriesChecker interface {
	// IsEphemeral checks if a series with the given labelset should be marked as ephemeral.
	IsEphemeral(mimirpb.WriteRequest_SourceEnum, []mimirpb.LabelAdapter) bool
}
