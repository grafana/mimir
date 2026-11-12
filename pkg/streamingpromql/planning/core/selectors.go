// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/timestamp"
)

// describeSelectorOptions holds the details of a selector for describeSelector to describe.
// Leave a field unset if it doesn't apply to the selector.
type describeSelectorOptions struct {
	Matchers              []LabelMatcher
	Timestamp             *time.Time
	Offset                time.Duration
	Range                 *time.Duration
	SkipHistogramBuckets  bool
	Anchored              bool
	Smoothed              bool
	CounterAware          bool
	ReturnStartTimestamps bool
	Subsets               []SubsetMatchers
}

func describeSelector(opts describeSelectorOptions) string {
	builder := &strings.Builder{}
	FormatMatchers(builder, opts.Matchers)

	if opts.Range != nil {
		builder.WriteRune('[')
		builder.WriteString(opts.Range.String())
		builder.WriteRune(']')
	}

	if opts.Timestamp != nil {
		builder.WriteString(" @ ")
		builder.WriteString(strconv.FormatInt(timestamp.FromTime(*opts.Timestamp), 10))
		builder.WriteString(" (")
		builder.WriteString(opts.Timestamp.Format(time.RFC3339Nano))
		builder.WriteRune(')')
	}

	if opts.Offset != 0 {
		builder.WriteString(" offset ")
		builder.WriteString(opts.Offset.String())
	}

	if opts.Anchored {
		builder.WriteString(" anchored")
	}
	if opts.Smoothed {
		builder.WriteString(" smoothed")
		if opts.CounterAware {
			builder.WriteString(" counter aware")
		}
	}

	if opts.SkipHistogramBuckets {
		builder.WriteString(", skip histogram buckets")
	}

	if opts.ReturnStartTimestamps {
		builder.WriteString(", return start timestamps")
	}

	if len(opts.Subsets) > 0 {
		builder.WriteString(", subsets: ")

		for i, subset := range opts.Subsets {
			if i > 0 {
				builder.WriteString(", ")
			}

			FormatMatchers(builder, subset.Filter)
			builder.WriteString(" (")
			FormatMatchers(builder, subset.AllMatchers)
			builder.WriteRune(')')
		}
	}

	return builder.String()
}
