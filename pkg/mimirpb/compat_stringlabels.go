// SPDX-License-Identifier: AGPL-3.0-only

//go:build stringlabels

package mimirpb

import (
	"github.com/prometheus/prometheus/model/labels"
)

// FromLabelAdaptersToLabels casts []LabelAdapter to labels.Labels.
// For now it's doing an expensive conversion: TODO figure out a faster way, maybe via PreallocTimeSeries.
func FromLabelAdaptersToLabels(ls []LabelAdapter) labels.Labels {
	builder := labels.NewScratchBuilder(len(ls))
	for _, v := range ls {
		builder.Add(v.Name, v.Value)
	}
	return builder.Labels()
}

// FromLabelAdaptersToLabelsWithCopy converts []LabelAdapter to labels.Labels.
// Do NOT use unsafe to convert between data types because this function may
// get in input labels whose data structure is reused.
func FromLabelAdaptersToLabelsWithCopy(input []LabelAdapter) labels.Labels {
	return FromLabelAdaptersToLabels(input).Copy()
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
