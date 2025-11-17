// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/value.go
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package functions

import (
	"fmt"
	"strings"

	"github.com/grafana/regexp"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func LabelJoinFactory(dstLabelOp, separatorOp types.StringOperator, srcLabelOps []types.StringOperator) SeriesMetadataFunction {
	return func(seriesMetadata []types.SeriesMetadata, tracker *limiter.MemoryConsumptionTracker, enableDelayedNameRemoval bool) ([]types.SeriesMetadata, error) {
		dst := dstLabelOp.GetValue()
		if !model.UTF8Validation.IsValidLabelName(dst) {
			return nil, fmt.Errorf("invalid destination label name in label_join(): %s", dst)
		}
		separator := separatorOp.GetValue()
		srcLabels := make([]string, len(srcLabelOps))
		for i, op := range srcLabelOps {
			src := op.GetValue()
			if !model.UTF8Validation.IsValidLabelName(src) {
				return nil, fmt.Errorf("invalid source label name in label_join(): %s", src)
			}
			srcLabels[i] = src
		}

		lb := labels.NewBuilder(labels.EmptyLabels())

		var sb strings.Builder
		for i := range seriesMetadata {
			sb.Reset()

			for j, srcLabel := range srcLabels {
				if j > 0 {
					sb.WriteString(separator)
				}
				// Get returns an empty string for missing labels, so this is safe and gives the desired output
				// where a series may be missing a source label.
				sb.WriteString(seriesMetadata[i].Labels.Get(srcLabel))
			}

			lb.Reset(seriesMetadata[i].Labels)
			lb.Set(dst, sb.String())
			tracker.DecreaseMemoryConsumptionForLabels(seriesMetadata[i].Labels)
			seriesMetadata[i].Labels = lb.Labels()
			err := tracker.IncreaseMemoryConsumptionForLabels(seriesMetadata[i].Labels)
			if err != nil {
				return nil, err
			}
		}

		if enableDelayedNameRemoval && dst == model.MetricNameLabel {
			for i := range seriesMetadata {
				seriesMetadata[i].DropName = false
			}
		}

		return seriesMetadata, nil
	}
}

func LabelReplaceFactory(dstLabelOp, replacementOp, srcLabelOp, regexOp types.StringOperator) SeriesMetadataFunction {
	return func(seriesMetadata []types.SeriesMetadata, tracker *limiter.MemoryConsumptionTracker, enableDelayedNameRemoval bool) ([]types.SeriesMetadata, error) {
		regexStr := regexOp.GetValue()
		regex, err := regexp.Compile("^(?s:" + regexStr + ")$")
		if err != nil {
			return nil, fmt.Errorf("invalid regular expression in label_replace(): %s", regexStr)
		}
		dst := dstLabelOp.GetValue()
		if !model.UTF8Validation.IsValidLabelName(dst) {
			return nil, fmt.Errorf("invalid destination label name in label_replace(): %s", dst)
		}
		repl := replacementOp.GetValue()
		src := srcLabelOp.GetValue()

		lb := labels.NewBuilder(labels.EmptyLabels())

		for i := range seriesMetadata {
			srcVal := seriesMetadata[i].Labels.Get(src)
			indexes := regex.FindStringSubmatchIndex(srcVal)
			if indexes != nil { // Only replace when regexp matches.
				res := regex.ExpandString([]byte{}, repl, srcVal, indexes)
				lb.Reset(seriesMetadata[i].Labels)
				lb.Set(dst, string(res))
				tracker.DecreaseMemoryConsumptionForLabels(seriesMetadata[i].Labels)
				seriesMetadata[i].Labels = lb.Labels()
				err := tracker.IncreaseMemoryConsumptionForLabels(seriesMetadata[i].Labels)
				if err != nil {
					return nil, err
				}
			}
		}

		if enableDelayedNameRemoval && dst == model.MetricNameLabel {
			for i := range seriesMetadata {
				seriesMetadata[i].DropName = false
			}
		}

		return seriesMetadata, nil
	}
}
