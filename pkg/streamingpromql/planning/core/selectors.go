// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/timestamp"
)

func describeSelector(
	matchers []*LabelMatcher,
	ts time.Time,
	offset time.Duration,
	rng *time.Duration,
	skipHistogramBuckets, anchored, smooothed, counterAware bool,
	subsets []SubsetMatchers,
) string {
	builder := &strings.Builder{}
	FormatMatchers(builder, matchers)

	if rng != nil {
		builder.WriteRune('[')
		builder.WriteString(rng.String())
		builder.WriteRune(']')
	}

	// The zero time.Time marks an unset stdtime field (no `@` modifier).
	if !ts.IsZero() {
		builder.WriteString(" @ ")
		builder.WriteString(strconv.FormatInt(timestamp.FromTime(ts), 10))
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

	if len(subsets) > 0 {
		builder.WriteString(", subsets: ")

		for i, subset := range subsets {
			if i > 0 {
				builder.WriteString(", ")
			}

			FormatMatchers(builder, subset.Matchers)
		}
	}

	return builder.String()
}
