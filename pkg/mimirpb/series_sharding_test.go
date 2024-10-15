// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/distributor/distributor.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package mimirpb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// This is not great, but we deal with unsorted labels in prePushRelabelMiddleware.
func TestShardByAllLabelAdaptersReturnsWrongResultsForUnsortedLabels(t *testing.T) {
	val1 := ShardByAllLabelAdapters("test", []LabelAdapter{
		{Name: "__name__", Value: "foo"},
		{Name: "bar", Value: "baz"},
		{Name: "sample", Value: "1"},
	})

	val2 := ShardByAllLabelAdapters("test", []LabelAdapter{
		{Name: "__name__", Value: "foo"},
		{Name: "sample", Value: "1"},
		{Name: "bar", Value: "baz"},
	})

	assert.NotEqual(t, val1, val2)
}
