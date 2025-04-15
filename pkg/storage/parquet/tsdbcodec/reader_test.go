// SPDX-License-Identifier: AGPL-3.0-only

package tsdbcodec

import (
	"testing"

	"github.com/prometheus/prometheus/tsdb"
)

func TestReader(t *testing.T) {
	tmpDir := t.TempDir()

	labelSets := GenerateTestLabelSets(DefaultMetricSizes, DefaultHistogramBuckets)
	storageSeries := GenerateTestStorageSeriesFromLabelSets(labelSets)

	tsdb.CreateBlock(storageSeries, tmpDir, 0, nil)

}
