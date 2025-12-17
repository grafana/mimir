// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
)

func describeSelector(matchers []*LabelMatcher, ts *time.Time, offset time.Duration, rng *time.Duration, skipHistogramBuckets, anchored, smooothed, counterAware bool, projectionLabels []string, projectionInclude bool) string {
	builder := &strings.Builder{}
	builder.WriteRune('{')
	for i, m := range matchers {
		if i > 0 {
			builder.WriteString(", ")
		}

		// Convert to the Prometheus type so we can use its String().
		promMatcher := labels.Matcher{Type: m.Type, Name: m.Name, Value: m.Value}
		builder.WriteString(promMatcher.String())
	}
	builder.WriteRune('}')

	if rng != nil {
		builder.WriteRune('[')
		builder.WriteString(rng.String())
		builder.WriteRune(']')
	}

	if ts != nil {
		builder.WriteString(" @ ")
		builder.WriteString(strconv.FormatInt(timestamp.FromTime(*ts), 10))
		builder.WriteString(" (")
		builder.WriteString(ts.Format(time.RFC3339Nano))
		builder.WriteRune(')')
	}

	if offset != 0 {
		builder.WriteString(" offset ")
		builder.WriteString(offset.String())
	}

	if anchored {
		builder.WriteString(" anchored")
	}
	if smooothed {
		builder.WriteString(" smoothed")
		if counterAware {
			builder.WriteString(" counter aware")
		}
	}

	if skipHistogramBuckets {
		builder.WriteString(", skip histogram buckets")
	}

	if len(projectionLabels) > 0 {
		if projectionInclude {
			builder.WriteString(`, include (`)
		} else {
			builder.WriteString(`, exclude (`)
		}

		quoted := make([]string, len(projectionLabels))
		for i := range projectionLabels {
			quoted[i] = strconv.Quote(projectionLabels[i])
		}

		builder.WriteString(strings.Join(quoted, `, `))
		builder.WriteString(`)`)
	}

	return builder.String()
}
