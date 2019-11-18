package chunk

import (
	"context"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/grafana/cortextool/pkg/chunk/filter"
)

// Scanner scans an
type Scanner interface {
	Scan(ctx context.Context, table string, fltr filter.MetricFilter, out chan chunk.Chunk) error
}
