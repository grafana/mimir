package chunk

import (
	"context"

	"github.com/cortexproject/cortex/pkg/chunk"
)

type Deleter interface {
	DeleteEntry(context.Context, chunk.IndexEntry) error
	DeleteSeries(context.Context, chunk.IndexQuery) ([]error, error)
}
