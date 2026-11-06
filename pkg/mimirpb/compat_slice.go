// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/cortexpb/compat.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

//go:build slicelabels

package mimirpb

import (
	"strings"
	"unsafe"

	"github.com/prometheus/prometheus/model/labels"
)

// UnsafeFromLabelAdaptersToLabels aliases label adapters as labels.Labels.
// The layouts are checked by tests. The result retains both the slice and any
// borrowed strings, and must not outlive them. It does not enforce label order.
func UnsafeFromLabelAdaptersToLabels(ls []LabelAdapter) labels.Labels {
	return *(*labels.Labels)(unsafe.Pointer(&ls))
}

// UnsafeFromLabelAdaptersOverwriteLabels replaces dest with labels that alias ls.
// The result must not outlive the input or its borrowed strings.
func UnsafeFromLabelAdaptersOverwriteLabels(_ *labels.ScratchBuilder, ls []LabelAdapter, dest *labels.Labels) {
	*dest = UnsafeFromLabelAdaptersToLabels(ls)
}

// FromLabelAdaptersToLabelsWithCopy converts label adapters to owned labels.
// The output retains neither the input slice nor any borrowed string storage.
func FromLabelAdaptersToLabelsWithCopy(input []LabelAdapter) labels.Labels {
	return CopyLabels(UnsafeFromLabelAdaptersToLabels(input))
}

// Copy data in Labels, such that any future Overwrite of input won't modify the returned value.
// We make a single block of bytes to hold all strings, to save memory compared to Go rounding up the allocations.
func CopyLabels(input []labels.Label) labels.Labels {
	result := make(labels.Labels, len(input))

	size := 0
	for _, l := range input {
		size += len(l.Name)
		size += len(l.Value)
	}

	var b strings.Builder
	b.Grow(size)
	for i, l := range input {
		b.WriteString(l.Name)
		result[i].Name = b.String()[b.Len()-len(l.Name):]
		b.WriteString(l.Value)
		result[i].Value = b.String()[b.Len()-len(l.Value):]
	}

	return result
}

// UnsafeFromLabelAdaptersToBuilder converts []LabelAdapter to labels.Builder.
func UnsafeFromLabelAdaptersToBuilder(ls []LabelAdapter, builder *labels.Builder) {
	builder.Reset(UnsafeFromLabelAdaptersToLabels(ls))
}

// FromBuilderToLabelAdapters converts labels.Builder to []LabelAdapter.
func FromBuilderToLabelAdapters(builder *labels.Builder, _ []LabelAdapter) []LabelAdapter {
	return FromLabelsToLabelAdapters(builder.Labels())
}

// FromLabelsToLabelAdapters aliases labels as label adapters.
// The identical layouts required by this conversion are checked by tests.
func FromLabelsToLabelAdapters(ls labels.Labels) []LabelAdapter {
	return *(*[]LabelAdapter)(unsafe.Pointer(&ls))
}

// CompareLabelAdapters returns 0 if a==b, <0 if a < b, and >0 if a > b.
func CompareLabelAdapters(a, b []LabelAdapter) int {
	return labels.Compare(UnsafeFromLabelAdaptersToLabels(a), UnsafeFromLabelAdaptersToLabels(b))
}
