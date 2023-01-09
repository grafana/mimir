// SPDX-License-Identifier: AGPL-3.0-only

package ephemeral

import "github.com/grafana/mimir/pkg/mimirpb"

type SeriesChecker interface {
	// IsEphemeral checks if a series with the given labelset should be marked as ephemeral.
	IsEphemeral([]mimirpb.LabelAdapter) bool
}
