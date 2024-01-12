// SPDX-License-Identifier: AGPL-3.0-only

package activeseries

import (
	"context"

	"github.com/prometheus/prometheus/tsdb/index"
)

type PostingsReader interface {
	Postings(ctx context.Context, name string, values ...string) (index.Postings, error)
}

func IsLabelValueActive(ctx context.Context, reader PostingsReader, activeSeries *ActiveSeries, name, value string) (bool, error) {
	valuePostings, err := reader.Postings(ctx, name, value)
	if err != nil {
		return false, err
	}

	activePostings := NewPostings(activeSeries, valuePostings)
	return activePostings.Next(), nil
}
