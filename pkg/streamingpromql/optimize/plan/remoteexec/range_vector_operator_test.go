// SPDX-License-Identifier: AGPL-3.0-only

package remoteexec

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"
)

func TestRangeVectorRemoteExec_FinishedReadingCalledAfterClosed(t *testing.T) {
	resp := &finishedReadingTestMockResponse{}

	o := &RangeVectorRemoteExec{
		Annotations: annotations.New(),
		resp:        resp,
	}

	o.Close()
	require.True(t, resp.Closed, "the response should have been closed")

	require.NoError(t, o.FinishedReading(context.Background()))
	require.False(t, resp.FinishedReadingCalled, "calling FinishedReading after Close should not try to read from the response stream")
}
