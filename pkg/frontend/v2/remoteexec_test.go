// SPDX-License-Identifier: AGPL-3.0-only

package v2

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/querierpb"
)

func TestRemoteExecutor(t *testing.T) {

}

// Errors returned in stream at any point
// GetEvaluationInfo: called when next message is an error
// GetEvaluationInfo: called when next message is EvaluationCompleted
// GetEvaluationInfo: called when next message is not EvaluationCompleted - discards messages until EvaluationCompleted found
// GetEvaluationInfo: called when next message is not EvaluationCompleted - discards messages until error found

func TestDecodeEvaluationCompletedMessage(t *testing.T) {
	msg := &querierpb.EvaluateQueryResponseEvaluationCompleted{
		Annotations: querierpb.Annotations{
			Warnings: []string{
				"warning: something isn't quite right",
				"warning: something else isn't quite right",
			},
			Infos: []string{
				"info: you should know about this",
				"info: you should know about this too",
			},
		},
		Stats: querierpb.QueryStats{
			TotalSamples: 1234,
		},
	}

	annos, totalSamples := decodeEvaluationCompletedMessage(msg)
	require.Equal(t, int64(1234), totalSamples)

	// If these tests fail, then the errors we're adding to the set of annotations likely
	// don't wrap PromQLInfo / PromQLWarning correctly.
	warnings, infos := annos.AsStrings("", 0, 0)
	require.ElementsMatch(t, []string{"warning: something isn't quite right", "warning: something else isn't quite right"}, warnings)
	require.ElementsMatch(t, []string{"info: you should know about this", "info: you should know about this too"}, infos)
}
