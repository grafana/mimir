// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/timestamp"
)

func describeSelector(matchers []*LabelMatcher, ts *time.Time, offset time.Duration, rng *time.Duration, skipHistogramBuckets, anchored, smooothed, counterAware bool, projectionLabels []string, projectionInclude bool) string {
	builder := &strings.Builder{}
	FormatMatchers(builder, matchers)

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

	// If we are excluding by no labels, don't display the projection section
	// since excluding by no labels is equivalent to not using projections.
	if len(projectionLabels) > 0 || projectionInclude {

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

func mergeProjectionLabels(include1 bool, lbls1 []string, include2 bool, lbls2 []string) (bool, []string) {
	// If the projection include/exclude differs between the two selectors, we can't
	// combine them so return "exclude" with an empty set of labels (equivalent to
	// projections not being used at all).
	if include1 != include2 {
		return false, []string{}
	}

	var (
		retInclude bool
		retLabels  []string
		unique     map[string]struct{}
	)

	if include1 {
		retInclude = true
		unique = unionLabels(lbls1, lbls2)
	} else {
		retInclude = false
		unique = intersectLabels(lbls1, lbls2)
	}

	retLabels = make([]string, 0, len(unique))
	for l := range unique {
		retLabels = append(retLabels, l)
	}

	slices.Sort(retLabels)
	return retInclude, retLabels
}

func unionLabels(lbls1 []string, lbls2 []string) map[string]struct{} {
	unique := make(map[string]struct{}, len(lbls1)+len(lbls2))
	for _, l := range lbls1 {
		unique[l] = struct{}{}
	}

	for _, l := range lbls2 {
		unique[l] = struct{}{}
	}

	return unique
}

func intersectLabels(lbls1 []string, lbls2 []string) map[string]struct{} {
	if len(lbls1) == 0 || len(lbls2) == 0 {
		return map[string]struct{}{}
	}

	lbls1Set := make(map[string]struct{}, len(lbls1))
	for _, l := range lbls1 {
		lbls1Set[l] = struct{}{}
	}

	unique := make(map[string]struct{})
	for _, l := range lbls2 {
		if _, ok := lbls1Set[l]; ok {
			unique[l] = struct{}{}
		}
	}

	return unique
}
