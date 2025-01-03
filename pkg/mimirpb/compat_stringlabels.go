// SPDX-License-Identifier: AGPL-3.0-only

//go:build stringlabels

package mimirpb

import (
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/mimirpb_custom"
)

// FromLabelAdaptersToLabels converts []LabelAdapter to labels.Labels.
// Note this is relatively expensive; see FromLabelAdaptersOverwriteLabels for a fast unsafe way.
func FromLabelAdaptersToLabels(ls []mimirpb_custom.LabelAdapter) labels.Labels {
	builder := labels.NewScratchBuilder(len(ls))
	for _, v := range ls {
		builder.Add(v.Name, v.Value)
	}
	return builder.Labels()
}

// FromLabelAdaptersToLabelsWithCopy converts []LabelAdapter to labels.Labels.
// The output does not retain any part of the input.
func FromLabelAdaptersToLabelsWithCopy(input []mimirpb_custom.LabelAdapter) labels.Labels {
	return FromLabelAdaptersToLabels(input)
}

// Copy data in Labels, such that any future Overwrite of input won't modify the returned value.
func CopyLabels(input labels.Labels) labels.Labels {
	return input.Copy()
}

// Build a labels.Labels from LabelAdaptors, with amortized zero allocations.
func FromLabelAdaptersOverwriteLabels(builder *labels.ScratchBuilder, ls []mimirpb_custom.LabelAdapter, dest *labels.Labels) {
	builder.Reset()
	for _, v := range ls {
		builder.Add(v.Name, v.Value)
	}
	builder.Overwrite(dest)
}

// FromLabelAdaptersToBuilder converts []LabelAdapter to labels.Builder.
func FromLabelAdaptersToBuilder(ls []mimirpb_custom.LabelAdapter, builder *labels.Builder) {
	builder.Reset(labels.EmptyLabels())
	for _, v := range ls {
		builder.Set(v.Name, v.Value)
	}
}

// FromBuilderToLabelAdapters converts labels.Builder to []LabelAdapter, reusing ls.
// Note the result may not be sorted.
func FromBuilderToLabelAdapters(builder *labels.Builder, ls []mimirpb_custom.LabelAdapter) []mimirpb_custom.LabelAdapter {
	ls = ls[:0]
	builder.Range(func(l labels.Label) {
		ls = append(ls, mimirpb_custom.LabelAdapter{Name: l.Name, Value: l.Value})
	})
	return ls
}

// FromLabelsToLabelAdapters casts labels.Labels to []LabelAdapter.
// For now it's doing an expensive conversion: TODO figure out a faster way.
func FromLabelsToLabelAdapters(ls labels.Labels) []mimirpb_custom.LabelAdapter {
	r := make([]mimirpb_custom.LabelAdapter, 0, ls.Len())
	ls.Range(func(l labels.Label) {
		r = append(r, mimirpb_custom.LabelAdapter{Name: l.Name, Value: l.Value})
	})
	return r
}

// CompareLabelAdapters returns be 0 if a==b, <0 if a < b, and >0 if a > b.
func CompareLabelAdapters(a, b []mimirpb_custom.LabelAdapter) int {
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
