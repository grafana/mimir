// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"errors"
	"fmt"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
)

// Series is a wrapper around index.Postings and tsdb.IndexReader. It implements
// the generic iterator interface to list all series in the index that are
// contained in the given postings.
type Series struct {
	postings index.Postings
	idx      tsdb.IndexReader
	buf      labels.ScratchBuilder
	err      error
}

func NewSeries(postings index.Postings, index tsdb.IndexReader) *Series {
	return &Series{
		postings: postings,
		idx:      index,
		buf:      labels.NewScratchBuilder(10),
	}
}

func (s *Series) Next() bool {
	return s.postings.Next()
}

func (s *Series) At() labels.Labels {
	s.buf.Reset()
	err := s.idx.Series(s.postings.At(), &s.buf, nil)
	if err != nil {
		s.err = fmt.Errorf("error getting series: %w", err)
		return labels.Labels{}
	}
	return s.buf.Labels()
}

func (s *Series) Err() error {
	if s.err != nil {
		return fmt.Errorf("error listing series: %w", errors.Join(s.err, s.postings.Err()))
	}
	return s.postings.Err()
}
