// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/distributor/distributor.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package mimirpb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShardByBorrowedMetricName(t *testing.T) {
	for _, metricName := range []string{"", "metric", "http_requests_total", "métric"} {
		assert.Equal(t, ShardByMetricName("tenant", metricName), ShardByBorrowedMetricName("tenant", BorrowString(metricName)))
	}
}

// This is not great, but we deal with unsorted labels in prePushRelabelMiddleware.
func TestShardByAllLabelAdaptersReturnsWrongResultsForUnsortedLabels(t *testing.T) {
	val1 := ShardByAllLabelAdapters("test", []LabelAdapter{
		{Name: BorrowString("__name__"), Value: BorrowString("foo")},
		{Name: BorrowString("bar"), Value: BorrowString("baz")},
		{Name: BorrowString("sample"), Value: BorrowString("1")},
	})

	val2 := ShardByAllLabelAdapters("test", []LabelAdapter{
		{Name: BorrowString("__name__"), Value: BorrowString("foo")},
		{Name: BorrowString("sample"), Value: BorrowString("1")},
		{Name: BorrowString("bar"), Value: BorrowString("baz")},
	})

	assert.NotEqual(t, val1, val2)
}
