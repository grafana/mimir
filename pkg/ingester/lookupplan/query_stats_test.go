// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueryStats_EstimatedFinalCardinality(t *testing.T) {
	t.Run("set and load estimated final cardinality", func(t *testing.T) {
		stats := &QueryStats{}
		stats.SetEstimatedFinalCardinality(100)
		stats.SetEstimatedFinalCardinality(50)

		assert.Equal(t, uint64(50), stats.LoadEstimatedFinalCardinality())
	})

	t.Run("set and load estimated final cardinality nil receiver", func(t *testing.T) {
		var stats *QueryStats
		stats.SetEstimatedFinalCardinality(100)

		assert.Equal(t, uint64(0), stats.LoadEstimatedFinalCardinality())
	})
}

func TestQueryStats_EstimatedSelectedPostings(t *testing.T) {
	t.Run("set and load estimated selected postings", func(t *testing.T) {
		stats := &QueryStats{}
		stats.SetEstimatedSelectedPostings(200)
		stats.SetEstimatedSelectedPostings(150)

		assert.Equal(t, uint64(150), stats.LoadEstimatedSelectedPostings())
	})

	t.Run("set and load estimated selected postings nil receiver", func(t *testing.T) {
		var stats *QueryStats
		stats.SetEstimatedSelectedPostings(200)

		assert.Equal(t, uint64(0), stats.LoadEstimatedSelectedPostings())
	})
}

func TestQueryStats_ActualSelectedPostings(t *testing.T) {
	t.Run("set and load actual selected postings", func(t *testing.T) {
		stats := &QueryStats{}
		stats.SetActualSelectedPostings(180)
		stats.SetActualSelectedPostings(120)

		assert.Equal(t, uint64(120), stats.LoadActualSelectedPostings())
	})

	t.Run("set and load actual selected postings nil receiver", func(t *testing.T) {
		var stats *QueryStats
		stats.SetActualSelectedPostings(180)

		assert.Equal(t, uint64(0), stats.LoadActualSelectedPostings())
	})
}

func TestQueryStats_ActualFinalCardinality(t *testing.T) {
	t.Run("set and load actual final cardinality", func(t *testing.T) {
		stats := &QueryStats{}
		stats.SetActualFinalCardinality(80)
		stats.SetActualFinalCardinality(60)

		assert.Equal(t, uint64(60), stats.LoadActualFinalCardinality())
	})

	t.Run("set and load actual final cardinality nil receiver", func(t *testing.T) {
		var stats *QueryStats
		stats.SetActualFinalCardinality(80)

		assert.Equal(t, uint64(0), stats.LoadActualFinalCardinality())
	})
}

func TestQueryStats_Context(t *testing.T) {
	t.Run("context with empty stats", func(t *testing.T) {
		stats := &QueryStats{}
		ctx := ContextWithQueryStats(context.Background(), stats)

		assert.NotNil(t, stats)
		retrievedStats := QueryStatsFromContext(ctx)
		assert.Equal(t, stats, retrievedStats)
	})

	t.Run("context without stats", func(t *testing.T) {
		ctx := context.Background()

		// When query statistics are enabled, the stats object is already initialised
		// within the context, so we can just check it.
		assert.Nil(t, QueryStatsFromContext(ctx))
	})
}

func TestQueryStats_AllFields(t *testing.T) {
	t.Run("comprehensive test with all fields", func(t *testing.T) {
		stats := &QueryStats{}

		stats.SetEstimatedFinalCardinality(1000)
		stats.SetEstimatedSelectedPostings(2000)
		stats.SetActualSelectedPostings(1800)
		stats.SetActualFinalCardinality(900)

		assert.Equal(t, uint64(1000), stats.LoadEstimatedFinalCardinality())
		assert.Equal(t, uint64(2000), stats.LoadEstimatedSelectedPostings())
		assert.Equal(t, uint64(1800), stats.LoadActualSelectedPostings())
		assert.Equal(t, uint64(900), stats.LoadActualFinalCardinality())
	})
}
