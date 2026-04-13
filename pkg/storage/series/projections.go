// SPDX-License-Identifier: AGPL-3.0-only

package series

import (
	"crypto/sha3"
	"encoding/base64"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/mimirpb"
)

const (
	HashLabelName = "__series_hash__"

	hashOutputSize = 16
)

type ProjectionHasher struct {
	buf  []byte
	hash *sha3.SHAKE
}

func NewProjectionHasher() *ProjectionHasher {
	return &ProjectionHasher{
		buf:  make([]byte, hashOutputSize),
		hash: sha3.NewSHAKE256(),
	}
}

func (p *ProjectionHasher) SeriesHash(lbls []mimirpb.LabelAdapter) (string, int) {
	p.hash.Reset()
	pos := 0

	for labelIdx, l := range lbls {
		if l.Name < HashLabelName {
			pos = labelIdx
		}

		_, _ = p.hash.Write([]byte(l.Name))
		_, _ = p.hash.Write([]byte(l.Value))
	}

	_, _ = p.hash.Read(p.buf)
	return base64.RawURLEncoding.EncodeToString(p.buf), pos + 1
}

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
