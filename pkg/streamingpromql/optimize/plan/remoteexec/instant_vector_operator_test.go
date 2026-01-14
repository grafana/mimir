// SPDX-License-Identifier: AGPL-3.0-only

package remoteexec

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"
)

func TestInstantVectorRemoteExec_FinalizeCalledAfterClosed(t *testing.T) {
	resp := &finalizationTestMockResponse{}

	o := &InstantVectorRemoteExec{
		Annotations: annotations.New(),
		resp:        resp,
	}

	o.Close()
	require.True(t, resp.Closed, "the response should have been closed")

	require.NoError(t, o.Finalize(context.Background()))
	require.False(t, resp.Finalized, "calling Finalize after Close should not try to read from the response stream")
}
