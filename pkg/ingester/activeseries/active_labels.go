package activeseries

import (
	"context"

	"github.com/prometheus/prometheus/tsdb/index"
)

type PostingsReader interface {
	Postings(ctx context.Context, name string, values ...string) (index.Postings, error)
}

func IsLabelValueActive(reader PostingsReader, activeSeries *ActiveSeries, name, value string) (bool, error) {
	valuePostings, err := reader.Postings(context.Background(), name, value)
	if err != nil {
		return false, err
	}

	activePostings := NewPostings(activeSeries, valuePostings)
	return activePostings.Next(), nil
}
