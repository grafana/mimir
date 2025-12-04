// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/cortexpb/compat.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/model/labels/labels_slicelabels.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

//go:build uniquelabels

package mimirpb

import (
	"unsafe"

	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/prometheus/model/labels"
)

// FromLabelAdaptersToLabels casts []LabelAdapter to []labels.Label and returns a labels.Labels instance.
// It uses unsafe, but as LabelAdapter == labels.Label this should be safe.
// This allows us to use labels.Labels directly in protos.
//
// Note: while resulting labels.Labels is supposedly sorted, this function
// doesn't enforce that. If input is not sorted, output will be wrong.
func FromLabelAdaptersToLabels(ls []LabelAdapter) labels.Labels {
	return labels.New(*(*[]labels.Label)(unsafe.Pointer(&ls))...)
}

// This is like FromLabelAdaptersToLabels but easier for stringlabels to implement.
func FromLabelAdaptersOverwriteLabels(_ *labels.ScratchBuilder, ls []LabelAdapter, dest *labels.Labels) {
	*dest = FromLabelAdaptersToLabels(ls)
}

// FromLabelAdaptersToLabelsWithCopy converts []LabelAdapter to labels.Labels.
// Do NOT use unsafe to convert between data types because this function may
// get input labels whose data structure is reused.
func FromLabelAdaptersToLabelsWithCopy(input []LabelAdapter) labels.Labels {
	// We can safely use FromLabelAdaptersToLabels because that calls labels.New, which clones
	// any label symbols not previously seen.
	return FromLabelAdaptersToLabels(input)
}

// Copy data in Labels, such that any future Overwrite of input won't modify the returned value.
// We make a single block of bytes to hold all strings, to save memory compared to Go rounding up the allocations.
func CopyLabels(input labels.Labels) labels.Labels {
	return input.Copy()
}

// FromLabelAdaptersToBuilder converts []LabelAdapter to labels.Builder.
func FromLabelAdaptersToBuilder(ls []LabelAdapter, builder *labels.Builder) {
	builder.Reset(FromLabelAdaptersToLabels(ls))
}

// FromBuilderToLabelAdapters converts labels.Builder to []LabelAdapter.
func FromBuilderToLabelAdapters(builder *labels.Builder, _ []LabelAdapter) []LabelAdapter {
	return FromLabelsToLabelAdapters(builder.Labels())
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

func FromLabelsToLabelAdaptersWithReuse(ls labels.Labels, reuse []LabelAdapter) []LabelAdapter {
	reuse = reuse[:0]
	ls.Range(func(l labels.Label) {
		reuse = append(reuse, LabelAdapter{Name: l.Name, Value: l.Value})
	})
	return reuse
}

// CompareLabelAdapters returns 0 if a==b, <0 if a < b, and >0 if a > b.
func CompareLabelAdapters(a, b []LabelAdapter) int {
	return labels.Compare(FromLabelAdaptersToLabels(a), FromLabelAdaptersToLabels(b))
}

const sep = '\xff'

var seps = []byte{sep}

// HashLabelAdaptors returns the same value as FromLabelAdaptersToLabels(lbls).Hash(),
// but without allocating a labels.Labels instance.
func HashLabelAdaptors(lbls []LabelAdapter) uint64 {
	// Copied from labels.Labels.Hash().

	// Use xxhash.Sum64(b) for fast path as it's faster.
	b := make([]byte, 0, 1024)
	for i, v := range lbls {
		if len(b)+len(v.Name)+len(v.Value)+2 >= cap(b) {
			// If labels entry is 1KB+ do not allocate whole entry.
			h := xxhash.New()
			_, _ = h.Write(b)
			for _, v := range lbls[i:] {
				_, _ = h.WriteString(v.Name)
				_, _ = h.Write(seps)
				_, _ = h.WriteString(v.Value)
				_, _ = h.Write(seps)
			}
			return h.Sum64()
		}

		b = append(b, v.Name...)
		b = append(b, sep)
		b = append(b, v.Value...)
		b = append(b, sep)
	}
	return xxhash.Sum64(b)
}
