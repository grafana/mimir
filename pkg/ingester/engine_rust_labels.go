// SPDX-License-Identifier: AGPL-3.0-only
//go:build rustengine && cgo

package ingester

import (
	"errors"
	"sync"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/rustengine"
)

type rustLabelSet struct {
	cursor         *rustQueryCursor
	count, index   uint64
	limit, yielded int
	page           []rustengine.Series
	at             storage.Series
	err            error
}

func (s *rustLabelSet) Next() bool {
	if err := s.cursor.ctx.Err(); err != nil {
		s.err = err
	}
	if s.err != nil || s.index >= s.count || (s.limit > 0 && s.yielded >= s.limit) {
		return false
	}
	if len(s.page) == 0 {
		limit := int(min(16, s.count-s.index))
		if s.limit > 0 {
			limit = min(limit, s.limit-s.yielded)
		}
		response, err := s.cursor.call(rustengine.Request{Op: "cursor_series_page", Ref: s.index, Limit: limit})
		if err != nil {
			s.err = err
			return false
		}
		if len(response.Series) == 0 || len(response.Series) > limit {
			s.err = errors.New("invalid Rust label page")
			return false
		}
		s.page = response.Series
	}
	s.at = &rustLabelSeries{adapters: s.page[0].Labels}
	s.page[0] = rustengine.Series{}
	s.page = s.page[1:]
	s.index++
	s.yielded++
	return true
}

func (s *rustLabelSet) At() storage.Series                { return s.at }
func (s *rustLabelSet) Err() error                        { return s.err }
func (s *rustLabelSet) Warnings() annotations.Annotations { return nil }

type rustLabelSeries struct {
	adapters []mimirpb.LabelAdapter
	once     sync.Once
	labels   labels.Labels
}

func (s *rustLabelSeries) Labels() labels.Labels {
	s.once.Do(func() { s.labels = rustLabels(s.adapters) })
	return s.labels
}
func (s *rustLabelSeries) LabelAdapters() []mimirpb.LabelAdapter { return s.adapters }
func (*rustLabelSeries) Iterator(chunkenc.Iterator) chunkenc.Iterator {
	return chunkenc.NewNopIterator()
}
