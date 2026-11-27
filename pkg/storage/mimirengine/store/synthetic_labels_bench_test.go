// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"fmt"
	"testing"

	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

func labelDebugEngine(b *testing.B, cold bool) *Engine {
	e, err := OpenEngine(b.TempDir(), "tenant", EngineOptions{Shards: 4, SecondaryHashFunction: secondaryHash})
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, e.Close()) })
	total := realisticEnvInt("MIMIR_LABEL_DEBUG_SERIES", 40_000)
	refs := make([]storage.SeriesRef, total)
	app := e.Appender(b.Context())
	for n := range total {
		metric := fmt.Sprintf("metric_%d", n%4000)
		if n%5 == 1 {
			metric = fmt.Sprintf("http_request_duration_%d", n%4000)
		}
		namespace, pod := fmt.Sprintf("namespace-%d", n%20), fmt.Sprintf("pod-%d", n)
		if n%50 == 0 {
			metric = "kube_pod_info"
			if n%5000 < 500 {
				namespace = fmt.Sprintf("synthetic-namespace-%d", (n/50)%16)
				pod = fmt.Sprintf("synthetic-worker-%d", n)
			}
		}
		pairs := []string{"__name__", metric, "cluster", fmt.Sprintf("cluster-%d", (n/50)%8), "namespace", namespace, "pod", pod, "instance", fmt.Sprintf("instance-%d", n%10000), "job", fmt.Sprintf("job-%d", n%40)}
		if n%10 == 0 {
			pairs = append(pairs, "synthetic_check_name", fmt.Sprintf("assertion-%d", (n/10)%64))
		}
		ref, err := app.Append(0, promlabels.FromStrings(pairs...), 0, 1000, 1, nil, nil, storage.AppendV2Options{})
		require.NoError(b, err)
		refs[n] = ref
		if n%2000 == 1999 {
			require.NoError(b, app.Commit())
			app = e.Appender(b.Context())
		}
	}
	require.NoError(b, app.Commit())
	if cold {
		require.NoError(b, e.CompactSelectedSeries(refs))
		app = e.Appender(b.Context())
		_, err := app.Append(0, promlabels.FromStrings("__name__", "current"), 0, 12*3_600_000, 1, nil, nil, storage.AppendV2Options{})
		require.NoError(b, err)
		require.NoError(b, app.Commit())
	}
	return e
}
