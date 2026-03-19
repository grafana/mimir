// SPDX-License-Identifier: AGPL-3.0-only

package series

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
)

const HashLabelName = "__series_hash__"

// ProjectionLabels modifies label sets for series to only retain labels required
// for a query and to ensure the set of labels continues to sort correctly compared
// to other label sets.
//
// This struct is _not_ concurrency safe.
type ProjectionLabels struct {
	builder *labels.Builder
	retain  map[string]struct{}
}

func NewProjectionLabels(names []string) *ProjectionLabels {
	retain := make(map[string]struct{}, len(names))
	for _, name := range names {
		retain[name] = struct{}{}
	}

	// We always retain the name of the metric and the unique series ID.
	retain[HashLabelName] = struct{}{}
	retain[model.MetricNameLabel] = struct{}{}

	return &ProjectionLabels{
		builder: labels.NewBuilder(labels.EmptyLabels()),
		retain:  retain,
	}
}

// Reduce returns a newly constructed set of labels based on the input labels that
// only contains the labels required for a particular query and to ensure that the
// set of labels continues to sort correctly compared to other label sets.
func (p *ProjectionLabels) Reduce(lbls labels.Labels) labels.Labels {
	p.builder.Reset(lbls)
	// Delete any labels that we haven't specifically been asked to keep if they appear _after_
	// the series hash. This ensures that this label set will still sort the same if we remove
	// the label in question.
	lbls.Range(func(l labels.Label) {
		_, ok := p.retain[l.Name]
		if !ok && l.Name > HashLabelName {
			p.builder.Del(l.Name)
		}
	})

	return p.builder.Labels()
}
