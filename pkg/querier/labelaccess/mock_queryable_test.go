// SPDX-License-Identifier: AGPL-3.0-only

package labelaccess

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

// mockQueryable is a simple queryable backed by configurable functions.
type mockQueryable struct {
	querier      func(mint, maxt int64) (storage.Querier, error)
	chunkQuerier func(mint, maxt int64) (storage.ChunkQuerier, error)
}

func (m *mockQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	return m.querier(mint, maxt)
}

func (m *mockQueryable) ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	return m.chunkQuerier(mint, maxt)
}

// mockSelectQuerier embeds storage.Querier and overrides Select via a closure.
type mockSelectQuerier struct {
	storage.Querier
	selectFn func(ctx context.Context, sorted bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet
}

func (m *mockSelectQuerier) Select(ctx context.Context, sorted bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	return m.selectFn(ctx, sorted, hints, matchers...)
}

func (m *mockSelectQuerier) Close() error { return nil }

// mockChunkSelectQuerier embeds storage.ChunkQuerier and overrides Select via a closure.
type mockChunkSelectQuerier struct {
	storage.ChunkQuerier
	selectFn func(ctx context.Context, sorted bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.ChunkSeriesSet
}

func (m *mockChunkSelectQuerier) Select(ctx context.Context, sorted bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.ChunkSeriesSet {
	return m.selectFn(ctx, sorted, hints, matchers...)
}

func (m *mockChunkSelectQuerier) Close() error { return nil }
