// SPDX-License-Identifier: AGPL-3.0-only

package rootqueryid

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const rootQueryID = "3f2b7c14-9d5a-4e61-8b0f-6a2c9d4e7f10"

func TestNew(t *testing.T) {
	first := New()
	second := New()

	require.NotEqual(t, first, second, "each query must get its own ID")

	// A parsable random UUID, rather than a counter value, so that reporting the ID to one tenant
	// does not disclose how many queries the query-frontend served for other tenants.
	parsed, err := uuid.Parse(first)
	require.NoError(t, err)
	require.Equal(t, uuid.Version(4), parsed.Version())
}

func TestIDFromContext(t *testing.T) {
	t.Run("round trips a root query ID", func(t *testing.T) {
		ctx := ContextWithID(t.Context(), rootQueryID)
		require.Equal(t, rootQueryID, IDFromContext(ctx))
		require.Equal(t,
			[]any{"existing", 1, FieldName, rootQueryID},
			AppendLogFields([]any{"existing", 1}, IDFromContext(ctx)))
	})

	t.Run("reports an empty ID when the context carries none", func(t *testing.T) {
		require.Empty(t, IDFromContext(t.Context()))
		require.Equal(t,
			[]any{"existing", 1},
			AppendLogFields([]any{"existing", 1}, IDFromContext(t.Context())),
			"an unknown root must not read as a real query")
	})

	t.Run("treats an explicitly empty ID as unknown", func(t *testing.T) {
		ctx := ContextWithID(t.Context(), "")
		require.Empty(t, IDFromContext(ctx))
		require.Empty(t, AppendLogFields(nil, IDFromContext(ctx)))
	})

	t.Run("the innermost value wins", func(t *testing.T) {
		ctx := ContextWithID(ContextWithID(t.Context(), "outer"), rootQueryID)
		require.Equal(t, rootQueryID, IDFromContext(ctx))
	})
}
