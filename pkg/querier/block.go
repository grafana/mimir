// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/block.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"fmt"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/querier/batch"
	"github.com/grafana/mimir/pkg/storage/chunk"
	"github.com/grafana/mimir/pkg/storage/series"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

func convertMatchersToLabelMatcher(matchers []*labels.Matcher) []storepb.LabelMatcher {
	var converted []storepb.LabelMatcher
	for _, m := range matchers {
		var t storepb.LabelMatcher_Type
		switch m.Type {
		case labels.MatchEqual:
			t = storepb.LabelMatcher_EQ
		case labels.MatchNotEqual:
			t = storepb.LabelMatcher_NEQ
		case labels.MatchRegexp:
			t = storepb.LabelMatcher_RE
		case labels.MatchNotRegexp:
			t = storepb.LabelMatcher_NRE
		}

		converted = append(converted, storepb.LabelMatcher{
			Type:  t,
			Name:  m.Name,
			Value: m.Value,
		})
	}
	return converted
}

func newBlockQuerierSeriesIterator(reuse chunkenc.Iterator, lbls labels.Labels, chunks []storepb.AggrChunk) chunkenc.Iterator {
	adapted := make([]chunk.Chunk, 0, len(chunks))

	for _, c := range chunks {
		encoding, ok := c.GetChunkEncoding()
		if !ok {
			return series.NewErrIterator(fmt.Errorf("cannot create new chunk for series %s: unknown encoded raw data type %v", lbls, c.Raw.Type))
		}

		encoded, err := chunk.NewForEncoding(encoding)
		if err != nil {
			return series.NewErrIterator(fmt.Errorf("cannot create new chunk for series %s: %s", lbls.String(), err.Error()))
		}

		if err := encoded.UnmarshalFromBuf(c.Raw.Data); err != nil {
			return series.NewErrIterator(fmt.Errorf("cannot unmarshal chunk for series %s: %s", lbls.String(), err.Error()))
		}

		adapted = append(adapted, chunk.NewChunk(lbls, encoded, model.Time(c.MinTime), model.Time(c.MaxTime)))
	}

	return batch.NewChunkMergeIterator(reuse, lbls, adapted)
}
