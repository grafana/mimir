// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/label_pairs.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"sort"
	"strings"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/extract"
)

// A series is uniquely identified by its set of label name/value
// pairs, which may arrive in any order over the wire
type labelPairs []mimirpb.LabelAdapter

func (a labelPairs) String() string {
	var b strings.Builder

	metricName, err := extract.MetricNameFromLabelAdapters(a)
	numLabels := len(a) - 1
	if err != nil {
		numLabels = len(a)
	}
	b.WriteString(metricName)
	b.WriteByte('{')
	count := 0
	for _, pair := range a {
		if pair.Name != model.MetricNameLabel {
			b.WriteString(pair.Name)
			b.WriteString("=\"")
			b.WriteString(pair.Value)
			b.WriteByte('"')
			count++
			if count < numLabels {
				b.WriteByte(',')
			}
		}
	}
	b.WriteByte('}')
	return b.String()
}

func valueForName(s labels.Labels, name string) (string, bool) {
	pos := sort.Search(len(s), func(i int) bool { return s[i].Name >= name })
	if pos == len(s) || s[pos].Name != name {
		return "", false
	}
	return s[pos].Value, true
}

// Check if a and b contain the same name/value pairs
func (a labelPairs) equal(b labels.Labels) bool {
	if len(a) != len(b) {
		return false
	}
	// Check as many as we can where the two sets are in the same order
	i := 0
	for ; i < len(a); i++ {
		if b[i].Name != string(a[i].Name) {
			break
		}
		if b[i].Value != string(a[i].Value) {
			return false
		}
	}
	// Now check remaining values using binary search
	for ; i < len(a); i++ {
		v, found := valueForName(b, a[i].Name)
		if !found || v != a[i].Value {
			return false
		}
	}
	return true
}
