// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/cortexpb/compat.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

//go:build !stringlabels

package mimirpb

import (
	"unsafe"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/mimirpb_custom"
)

// FromLabelAdaptersToLabels casts []LabelAdapter to labels.Labels.
// It uses unsafe, but as LabelAdapter == labels.Label this should be safe.
// This allows us to use labels.Labels directly in protos.
//
// Note: while resulting labels.Labels is supposedly sorted, this function
// doesn't enforce that. If input is not sorted, output will be wrong.
func FromLabelAdaptersToLabels(ls []mimirpb_custom.LabelAdapter) labels.Labels {
	return *(*labels.Labels)(unsafe.Pointer(&ls))
}

// This is like FromLabelAdaptersToLabels but easier for stringlabels to implement.
func FromLabelAdaptersOverwriteLabels(_ *labels.ScratchBuilder, ls []mimirpb_custom.LabelAdapter, dest *labels.Labels) {
	*dest = FromLabelAdaptersToLabels(ls)
}

// FromLabelAdaptersToLabelsWithCopy converts []LabelAdapter to labels.Labels.
// Do NOT use unsafe to convert between data types because this function may
// get in input labels whose data structure is reused.
func FromLabelAdaptersToLabelsWithCopy(input []mimirpb_custom.LabelAdapter) labels.Labels {
	return CopyLabels(FromLabelAdaptersToLabels(input))
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

	// Copy all strings into the buffer, and use 'yoloString' to convert buffer
	// slices to strings.
	buf := make([]byte, size)

	for i, l := range input {
		result[i].Name, buf = copyStringToBuffer(l.Name, buf)
		result[i].Value, buf = copyStringToBuffer(l.Value, buf)
	}
	return result
}

// Copies string to buffer (which must be big enough), and converts buffer slice containing
// the string copy into new string.
func copyStringToBuffer(in string, buf []byte) (string, []byte) {
	l := len(in)
	c := copy(buf, in)
	if c != l {
		panic("not copied full string")
	}

	return yoloString(buf[0:l]), buf[l:]
}

// FromLabelAdaptersToBuilder converts []LabelAdapter to labels.Builder.
func FromLabelAdaptersToBuilder(ls []mimirpb_custom.LabelAdapter, builder *labels.Builder) {
	builder.Reset(FromLabelAdaptersToLabels(ls))
}

// FromBuilderToLabelAdapters converts labels.Builder to []LabelAdapter.
func FromBuilderToLabelAdapters(builder *labels.Builder, _ []mimirpb_custom.LabelAdapter) []mimirpb_custom.LabelAdapter {
	return FromLabelsToLabelAdapters(builder.Labels())
}

// FromLabelsToLabelAdapters casts labels.Labels to []LabelAdapter.
// It uses unsafe, but as LabelAdapter == labels.Label this should be safe.
// This allows us to use labels.Labels directly in protos.
func FromLabelsToLabelAdapters(ls labels.Labels) []mimirpb_custom.LabelAdapter {
	return *(*[]mimirpb_custom.LabelAdapter)(unsafe.Pointer(&ls))
}

// CompareLabelAdapters returns 0 if a==b, <0 if a < b, and >0 if a > b.
func CompareLabelAdapters(a, b []mimirpb_custom.LabelAdapter) int {
	return labels.Compare(FromLabelAdaptersToLabels(a), FromLabelAdaptersToLabels(b))
}
