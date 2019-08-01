package tool

import (
	"context"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/grafana/cortex-tool/pkg/chunk/filter"
)

// Scanner scans an
type Scanner interface {
	Scan(ctx context.Context, table string, fltr filter.MetricFilter, out chan chunk.Chunk) error
}

type Deleter interface {
	DeleteEntry(ctx context.Context, entry chunk.IndexEntry) error
}
