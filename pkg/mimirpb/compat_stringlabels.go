// SPDX-License-Identifier: AGPL-3.0-only

//go:build !slicelabels && !dedupelabels

package mimirpb

import (
	"unsafe"

	"github.com/prometheus/prometheus/model/labels"
)

// UnsafeFromLabelAdaptersToLabels converts label adapters to labels and sorts them.
// Callers must not assume ownership of borrowed input strings. This implementation
// copies into the compact labels representation; other builds may retain input.
func UnsafeFromLabelAdaptersToLabels(ls []LabelAdapter) labels.Labels {
	// LabelAdapter and labels.Label have identical memory layouts, checked by tests.
	l := *(*[]labels.Label)(unsafe.Pointer(&ls))
	return labels.New(l...)
}

// FromLabelAdaptersToLabelsWithCopy converts []LabelAdapter to owned labels.
// The output does not retain any part of the input.
func FromLabelAdaptersToLabelsWithCopy(input []LabelAdapter) labels.Labels {
	return UnsafeFromLabelAdaptersToLabels(input)
}

// CopyLabels copies labels so overwriting input cannot modify the result.
func CopyLabels(input labels.Labels) labels.Labels {
	return input.Copy()
}

// UnsafeFromLabelAdaptersOverwriteLabels builds labels with amortized zero allocations.
// Callers must not retain borrowed input beyond its lifetime.
func UnsafeFromLabelAdaptersOverwriteLabels(builder *labels.ScratchBuilder, ls []LabelAdapter, dest *labels.Labels) {
	builder.Reset()
	for _, v := range ls {
		builder.Add(v.Name.value, v.Value.value)
	}
	builder.Overwrite(dest)
}

// UnsafeFromLabelAdaptersToBuilder populates a builder that may retain borrowed strings.
func UnsafeFromLabelAdaptersToBuilder(ls []LabelAdapter, builder *labels.Builder) {
	builder.Reset(labels.EmptyLabels())
	for _, v := range ls {
		builder.Set(v.Name.value, v.Value.value)
	}
}

// FromBuilderToLabelAdapters converts labels.Builder to []LabelAdapter, reusing ls.
// Note the result may not be sorted.
func FromBuilderToLabelAdapters(builder *labels.Builder, ls []LabelAdapter) []LabelAdapter {
	ls = ls[:0]
	builder.Range(func(l labels.Label) {
		ls = append(ls, LabelAdapter{Name: BorrowString(l.Name), Value: BorrowString(l.Value)})
	})
	return ls
}

// FromLabelsToLabelAdapters converts labels to label adapters.
func FromLabelsToLabelAdapters(ls labels.Labels) []LabelAdapter {
	r := make([]LabelAdapter, 0, ls.Len())
	ls.Range(func(l labels.Label) {
		r = append(r, LabelAdapter{Name: BorrowString(l.Name), Value: BorrowString(l.Value)})
	})
	return r
}

// CompareLabelAdapters returns 0 if a==b, <0 if a < b, and >0 if a > b.
func CompareLabelAdapters(a, b []LabelAdapter) int {
	l := min(len(a), len(b))
	for i := 0; i < l; i++ {
		if c := a[i].Compare(b[i]); c != 0 {
			return c
		}
	}
	return len(a) - len(b)
}
