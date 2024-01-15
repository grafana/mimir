// SPDX-License-Identifier: AGPL-3.0-only

package iterators

import (
	"errors"
	"unicode/utf8"

	"github.com/bboreham/go-loser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
)

// MergedLabelValues is a label values iterator merging a collection of sub-iterators.
type MergedLabelValues struct {
	lt  *loser.Tree[string, storage.LabelValues]
	its []storage.LabelValues
	cur string
}

func (m *MergedLabelValues) Next() bool {
	for m.lt.Next() {
		// Remove duplicate entries
		at := m.lt.At()
		if at != m.cur {
			m.cur = at
			return true
		}
	}

	return false
}

func (m *MergedLabelValues) At() string {
	return m.cur
}

func (m *MergedLabelValues) Err() error {
	for _, it := range m.its {
		if err := it.Err(); err != nil {
			return err
		}
	}
	return nil
}

func (m *MergedLabelValues) Warnings() annotations.Annotations {
	var warnings annotations.Annotations
	for _, it := range m.its {
		warnings = warnings.Merge(it.Warnings())
	}
	return warnings
}

func (m *MergedLabelValues) Close() error {
	var errs []error
	for _, it := range m.its {
		if err := it.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func NewMergedLabelValues(its []storage.LabelValues) storage.LabelValues {
	lt := loser.New(its, string(utf8.MaxRune))
	return &MergedLabelValues{
		lt:  lt,
		its: its,
	}
}
