// SPDX-License-Identifier: AGPL-3.0-only

package ephemeral

import "github.com/grafana/mimir/pkg/mimirpb"

type SeriesCheckerByUser interface {
	// EphemeralChecker returns an object which checks a series and decides whether
	// this series should be ephemeral based on the given user's configuration.
	// Can return nil if the user has no relevant configuration.
	EphemeralChecker(string) SeriesChecker
}

type SeriesChecker interface {
	// ShouldMarkEphemeral checks if a series with the given labelset should be marked as ephemeral.
	ShouldMarkEphemeral(mimirpb.WriteRequest_SourceEnum, []mimirpb.LabelAdapter) bool

	// HasMatchers returns true if at least one matcher is defined, otherwise it returns false.
	// This allows callers to shortcut their logic if there are no matchers defined.
	HasMatchers() bool
}
