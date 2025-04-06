// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
)

func describeSelector(matchers []*LabelMatcher, ts *Timestamp, offset time.Duration, rng *time.Duration) string {
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
		builder.WriteString(strconv.FormatInt(ts.Timestamp, 10))
		builder.WriteString(" (")
		builder.WriteString(timestamp.Time(ts.Timestamp).Format(time.RFC3339Nano))
		builder.WriteRune(')')
	}

	if offset != 0 {
		builder.WriteString(" offset ")
		builder.WriteString(offset.String())
	}

	return builder.String()
}
