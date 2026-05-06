// SPDX-License-Identifier: AGPL-3.0-only

//go:build !slicelabels && !dedupelabels

package mimirpb

import (
	"unsafe"

	"github.com/prometheus/prometheus/model/labels"
)

// FromLabelAdaptersToLabels casts []LabelAdapter to labels.Labels and sorts the Labels. It uses unsafe.
func FromLabelAdaptersToLabels(ls []LabelAdapter) labels.Labels {
	l := *(*[]labels.Label)(unsafe.Pointer(&ls))
	return labels.New(l...)
}

// FromLabelAdaptersToLabelsWithCopy converts []LabelAdapter to labels.Labels.
// The output does not retain any part of the input.
func FromLabelAdaptersToLabelsWithCopy(input []LabelAdapter) labels.Labels {
	return FromLabelAdaptersToLabels(input)
}

// Copy data in Labels, such that any future Overwrite of input won't modify the returned value.
func CopyLabels(input labels.Labels) labels.Labels {
	return input.Copy()
}

// Build a labels.Labels from LabelAdapters, with amortized zero allocations.
func FromLabelAdaptersOverwriteLabels(builder *labels.ScratchBuilder, ls []LabelAdapter, dest *labels.Labels) {
	builder.Reset()
	for _, v := range ls {
		builder.Add(v.Name, v.Value)
	}
	builder.Overwrite(dest)
}

// FromLabelAdaptersToBuilder converts []LabelAdapter to labels.Builder.
func FromLabelAdaptersToBuilder(ls []LabelAdapter, builder *labels.Builder) {
	builder.Reset(labels.EmptyLabels())
	for _, v := range ls {
		builder.Set(v.Name, v.Value)
	}
}

// FromBuilderToLabelAdapters converts labels.Builder to []LabelAdapter, reusing ls.
// Note the result may not be sorted.
func FromBuilderToLabelAdapters(builder *labels.Builder, ls []LabelAdapter) []LabelAdapter {
	ls = ls[:0]
	builder.Range(func(l labels.Label) {
		ls = append(ls, LabelAdapter{Name: l.Name, Value: l.Value})
	})
	return ls
}

// FromLabelsToLabelAdapters casts labels.Labels to []LabelAdapter.
// For now it's doing an expensive conversion: TODO figure out a faster way.
func FromLabelsToLabelAdapters(ls labels.Labels) []LabelAdapter {
	r := make([]LabelAdapter, 0, ls.Len())
	ls.Range(func(l labels.Label) {
		r = append(r, LabelAdapter{Name: l.Name, Value: l.Value})
	})
	return r
}

// AppendFromLabelsToLabelAdapters appends label adapters converted from ls to the
// dst slice. It reuses the capacity of dst when possible to avoid allocations.
//
// When dst has sufficient remaining capacity for all labels, this avoids
// calling ls.Len() (which is O(n) for stringlabels, as it must traverse
// the packed label data). In the common case — dst from a pool with
// pre-allocated space for at least 20 labels — this saves a full traversal.
//
// When capacity is exhausted during iteration, it calls ls.Len() once to
// compute the exact remaining count and re-allocates precisely, avoiding
// the repeated grow-and-copy overhead of Go's append doubling strategy.
func AppendFromLabelsToLabelAdapters(dst []LabelAdapter, ls labels.Labels) []LabelAdapter {
	startLen := len(dst)
	ls.Range(func(l labels.Label) {
		if len(dst) == cap(dst) {
			// Capacity exhausted: compute exact remaining count and
			// allocate once with the right size. The total number of
			// labels is ls.Len(); we've already appended (len(dst) - startLen).
			totalLabels := ls.Len()
			remaining := totalLabels - (len(dst) - startLen)
			newDst := make([]LabelAdapter, len(dst), len(dst)+remaining)
			copy(newDst, dst)
			dst = newDst
		}
		dst = append(dst, LabelAdapter{Name: l.Name, Value: l.Value})
	})
	return dst
}

// CompareLabelAdapters returns be 0 if a==b, <0 if a < b, and >0 if a > b.
func CompareLabelAdapters(a, b []LabelAdapter) int {
	l := len(a)
	if len(b) < l {
		l = len(b)
	}

	for i := 0; i < l; i++ {
		if a[i].Name != b[i].Name {
			if a[i].Name < b[i].Name {
				return -1
			}
			return 1
		}
		if a[i].Value != b[i].Value {
			if a[i].Value < b[i].Value {
				return -1
			}
			return 1
		}
	}
	// If all labels so far were in common, the set with fewer labels comes first.
	return len(a) - len(b)
}
