// SPDX-License-Identifier: AGPL-3.0-only

package cardpoc

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/tools/headcount/cardgen"
	"github.com/grafana/mimir/tools/headcount/model"
)

func TestRunE1_ExactOnGeneratedBlocks(t *testing.T) {
	pop, err := model.New(model.Config{
		Seed:        1,
		Start:       time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC),
		End:         time.Date(2026, 9, 20, 5, 0, 0, 0, time.UTC),
		MetricNames: 25,
		SeriesZipfS: 1.2, SeriesFloor: 1, SeriesCap: 40,
		ChurnFraction: 0.2, ChurnPeriod: 2 * time.Hour,
		GapFraction: 0.1, GapDuration: time.Hour,
		StaleFraction: 0.1,
		SpikeMetric:   0, SpikeDay: 0, SpikeBaseValues: 3, SpikePeakValues: 50,
	})
	require.NoError(t, err)

	bucket := t.TempDir()
	require.NoError(t, os.MkdirAll(bucket+"/anonymous", 0o755))
	metas, err := cardgen.Generate(pop, bucket+"/anonymous", cardgen.Config{Partitions: 3, Seed: 1})
	require.NoError(t, err)
	require.NotEmpty(t, metas)

	results, err := RunE1(bucket)
	require.NoError(t, err)
	require.Len(t, results, len(metas))

	var totalChecked int
	for _, res := range results {
		require.True(t, res.Exact(), "block %s: no-matcher=%q mismatches=%v", res.Block, res.NoMatcherMismatch, res.Mismatches)
		totalChecked += res.ValuesChecked
	}
	require.Greater(t, totalChecked, 0, "the test population must exercise at least one label value")
}
