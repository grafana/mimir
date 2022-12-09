// SPDX-License-Identifier: AGPL-3.0-only

//go:build stringlabels

package mimirpb

import (
	"github.com/prometheus/prometheus/model/labels"
)

// FromLabelAdaptersToLabels converts []LabelAdapter to labels.Labels.
// Note this is relatively expensive; see FromLabelAdaptersOverwriteLabels for a fast unsafe way.
func FromLabelAdaptersToLabels(ls []LabelAdapter) labels.Labels {
	builder := labels.NewScratchBuilder(len(ls))
	for _, v := range ls {
		builder.Add(v.Name, v.Value)
	}
	return builder.Labels()
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

// Build a labels.Labels from LabelAdaptors, with amortized zero allocations.
func FromLabelAdaptersOverwriteLabels(builder *labels.ScratchBuilder, ls []LabelAdapter, dest *labels.Labels) {
	builder.Reset()
	for _, v := range ls {
		builder.Add(v.Name, v.Value)
	}
	builder.Overwrite(dest)
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
