// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContextWithOnlyReplica(t *testing.T) {
	t.Run("when only replica exists in a context", func(t *testing.T) {
		ctx := ContextWithOnlyReplica(context.Background())
		require.True(t, IsOnlyReplicaContext(ctx))
	})

	t.Run("when is only replica does not exist in a context", func(t *testing.T) {
		require.False(t, IsOnlyReplicaContext(context.Background()))
	})
}
