// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
)

func describeSelector(matchers []*LabelMatcher, ts *time.Time, offset time.Duration, rng *time.Duration, skipHistogramBuckets bool, forCacheKey bool) string {
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
		if !forCacheKey {
			// Only include human-readable timestamp for display purposes (redundant with unix timestamp)
			builder.WriteString(" (")
			builder.WriteString(ts.Format(time.RFC3339Nano))
			builder.WriteRune(')')
		}
	}

	if offset != 0 {
		builder.WriteString(" offset ")
		builder.WriteString(offset.String())
	}

	if skipHistogramBuckets {
		// This needs to be kept in the cache key, as the returned results will differ depending on if buckets are skipped or not
		builder.WriteString(", skip histogram buckets")
	}

	return builder.String()
}
