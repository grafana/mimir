// SPDX-License-Identifier: AGPL-3.0-only

package limits

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMergesLikeDskit(t *testing.T) {
	m := func(document string) map[string]any {
		parsed, err := ParseDocument([]byte(document))
		require.NoError(t, err)
		return parsed
	}
	merged, err := MergeMaps(
		m("overrides:\n  a:\n    x: 1\n    y: 2\n  b: {z: 1}\nother: 1\n"),
		m(`{"overrides": {"a": {"y": 3}, "c": {"w": 1}}}`), "")
	require.NoError(t, err)
	flatten := func(v any) any {
		// JSON numbers and YAML ints compare by their value.
		return normalizeNumbers(v)
	}
	require.Equal(t, flatten(map[string]any{
		"overrides": map[string]any{"a": map[string]any{"x": 1, "y": 3}, "b": map[string]any{"z": 1}, "c": map[string]any{"w": 1}},
		"other":     1,
	}), flatten(merged))
	nulls, err := MergeMaps(m("overrides:\n"), m("overrides:\n  a: {x: 1}\n"), "")
	require.NoError(t, err)
	require.Equal(t, flatten(map[string]any{"overrides": map[string]any{"a": map[string]any{"x": 1}}}), flatten(nulls))
	_, err = MergeMaps(m("a: 1\n"), m("a: {b: 1}\n"), "")
	require.Error(t, err)
}

func normalizeNumbers(v any) any {
	switch v := v.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, x := range v {
			out[k] = normalizeNumbers(x)
		}
		return out
	default:
		if i, err := intValue(v); err == nil {
			return i
		}
		return v
	}
}

func TestLoadsFilesAndURLsLeftToRightAndSkipsUnchanged(t *testing.T) {
	first := filepath.Join(t.TempDir(), "overrides.yaml")
	require.NoError(t, os.WriteFile(first, []byte("overrides:\n  t:\n    max_global_exemplars_per_user: 5\n    max_global_metadata_per_user: 7\n"), 0o644))
	var seenMu sync.Mutex
	var seenCluster string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenMu.Lock()
		seenCluster = r.Header.Get("X-Cluster")
		seenMu.Unlock()
		_, _ = w.Write([]byte(`{"overrides":{"t":{"max_global_exemplars_per_user":9}}}`))
	}))
	defer server.Close()
	config, err := NewRuntimeConfig(RuntimeConfigArgs{
		File:                   first + ", " + server.URL + "/overrides",
		ReloadPeriod:           "10s",
		HTTPClientTimeout:      "5s",
		ClusterValidationLabel: "cell",
	})
	require.NoError(t, err)
	overrides := DefaultOverrides()
	changed, err := config.Load(context.Background(), overrides)
	require.NoError(t, err)
	require.True(t, changed)
	l := overrides.Tenant("t")
	require.Equal(t, int64(9), l.Limits.MaxGlobalExemplarsPerUser)
	require.Equal(t, int64(7), l.Limits.MaxGlobalMetadataPerUser)
	seenMu.Lock()
	require.Equal(t, "cell", seenCluster)
	seenMu.Unlock()
	changed, err = config.Load(context.Background(), overrides)
	require.NoError(t, err)
	require.False(t, changed)
	// A broken source fails the load and keeps the previous overrides.
	require.NoError(t, os.WriteFile(first, []byte("overrides: ["), 0o644))
	_, err = config.Load(context.Background(), overrides)
	require.Error(t, err)
	require.Equal(t, int64(9), overrides.Tenant("t").Limits.MaxGlobalExemplarsPerUser)
}
