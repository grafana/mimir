// SPDX-License-Identifier: AGPL-3.0-only

package ephemeral

import "github.com/grafana/mimir/pkg/mimirpb"

type EphemeralCheckerByUser interface {
	// EphemeralChecker returns an object which checks a labelset and decides whether
	// this series should be ephemeral based on the given user's configuration.
	EphemeralChecker(string) EphemeralChecker
}

type EphemeralChecker interface {
	// IsEphemeral checks if a series with the given labelset should be marked as ephemeral.
	IsEphemeral([]mimirpb.LabelAdapter) bool
}
