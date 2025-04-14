// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/26344c3ec7409713fcf52a9c41cd0dce537b3100/pkg/storage/parquet/stats_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package storage

import (
	"os"
	"testing"

	"github.com/go-kit/log"
)

func TestDisplay(t *testing.T) {
	stats := NewStats()
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	extraFields := []interface{}{
		"request", "labelNames",
		"mint", 0,
		"maxt", 100000,
	}
	stats.AddColumnSearched("name", 1000)
	stats.AddPagesSearchSizeBytes("job", 100000)
	stats.Display(logger, extraFields...)
}
