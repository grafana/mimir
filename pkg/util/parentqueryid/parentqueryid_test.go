// SPDX-License-Identifier: AGPL-3.0-only

package parentqueryid

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIDFromContext(t *testing.T) {
	// Parent query IDs are seeded from rand.Uint64(), so roughly half are above math.MaxInt64.
	const parentQueryID = uint64(math.MaxUint64) - 12345

	t.Run("round trips a parent query ID", func(t *testing.T) {
		ctx := ContextWithID(t.Context(), parentQueryID)
		require.Equal(t, parentQueryID, IDFromContext(ctx))
		require.Equal(t,
			[]any{"existing", 1, FieldName, parentQueryID},
			AppendLogFields([]any{"existing", 1}, IDFromContext(ctx)))
	})

	t.Run("reports zero when the context carries no parent query ID", func(t *testing.T) {
		require.Zero(t, IDFromContext(t.Context()))
		require.Equal(t,
			[]any{"existing", 1},
			AppendLogFields([]any{"existing", 1}, IDFromContext(t.Context())),
			"an unknown parent must not be reported as parent query 0")
	})

	t.Run("treats an explicit zero as unknown", func(t *testing.T) {
		ctx := ContextWithID(t.Context(), 0)
		require.Zero(t, IDFromContext(ctx))
		require.Empty(t, AppendLogFields(nil, IDFromContext(ctx)))
	})

	t.Run("the innermost value wins", func(t *testing.T) {
		ctx := ContextWithID(ContextWithID(t.Context(), 1), parentQueryID)
		require.Equal(t, parentQueryID, IDFromContext(ctx))
	})
}
