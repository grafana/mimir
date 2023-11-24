package util

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// This is not great, but we deal with unsorted labels in prePushRelabelMiddleware.
func TestShardByAllLabelAdaptersReturnsWrongResultsForUnsortedLabels(t *testing.T) {
	val1 := ShardByAllLabelAdapters("test", []mimirpb.LabelAdapter{
		{Name: "__name__", Value: "foo"},
		{Name: "bar", Value: "baz"},
		{Name: "sample", Value: "1"},
	})

	val2 := ShardByAllLabelAdapters("test", []mimirpb.LabelAdapter{
		{Name: "__name__", Value: "foo"},
		{Name: "sample", Value: "1"},
		{Name: "bar", Value: "baz"},
	})

	assert.NotEqual(t, val1, val2)
}
