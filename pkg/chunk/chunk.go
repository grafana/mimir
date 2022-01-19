// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/chunk/chunk.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package chunk

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	prom_chunk "github.com/grafana/mimir/pkg/chunk/encoding"
)

// Chunk contains encoded timeseries data
type Chunk struct {
	From    model.Time       `json:"from"`
	Through model.Time       `json:"through"`
	Metric  labels.Labels    `json:"metric"`
	Data    prom_chunk.Chunk `json:"-"`
}

// NewChunk creates a new chunk
func NewChunk(metric labels.Labels, c prom_chunk.Chunk, from, through model.Time) Chunk {
	return Chunk{
		From:    from,
		Through: through,
		Metric:  metric,
		Data:    c,
	}
}

// Samples returns all SamplePairs for the chunk.
func (c *Chunk) Samples(from, through model.Time) ([]model.SamplePair, error) {
	it := c.Data.NewIterator(nil)
	interval := prom_chunk.Interval{OldestInclusive: from, NewestInclusive: through}
	return prom_chunk.RangeValues(it, interval)
}
