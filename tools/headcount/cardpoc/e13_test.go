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

func TestMeasureNameTables_BoundedByEntryLayout(t *testing.T) {
	start := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	pop, err := model.New(model.Config{
		Seed:        1,
		Start:       start,
		End:         start.Add(time.Hour),
		MetricNames: 200,
		SeriesZipfS: 1.2, SeriesFloor: 1, SeriesCap: 10,
		SpikeMetric: -1,
	})
	require.NoError(t, err)
	bucket := t.TempDir()
	require.NoError(t, os.MkdirAll(bucket+"/anonymous", 0o755))
	_, err = cardgen.Generate(pop, bucket+"/anonymous", cardgen.Config{Partitions: 1, Seed: 1})
	require.NoError(t, err)

	tables, err := MeasureNameTables(bucket)
	require.NoError(t, err)
	require.Len(t, tables, 1)
	tbl := tables[0]
	require.Equal(t, 200, tbl.Names)

	// Each entry is: key count (1 byte), "__name__" with its length (9),
	// the value with its length (1 + len), and a postings offset varint
	// (1 to 5 bytes for these small files).
	var valueBytes int64
	names := map[string]bool{}
	for _, s := range pop.Series {
		names[s.Labels.Get("__name__")] = true
	}
	for n := range names {
		valueBytes += int64(len(n))
	}
	require.GreaterOrEqual(t, tbl.Bytes, valueBytes+int64(tbl.Names)*12)
	require.LessOrEqual(t, tbl.Bytes, valueBytes+int64(tbl.Names)*16)
	require.Less(t, tbl.Bytes, tbl.TableBytes, "the table also holds the instance label's entries")
}
