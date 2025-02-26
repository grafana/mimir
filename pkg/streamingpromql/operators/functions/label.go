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

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func LabelJoinFactory(dstLabelOp, separatorOp types.StringOperator, srcLabelOps []types.StringOperator) SeriesMetadataFunction {
	return func(seriesMetadata []types.SeriesMetadata, _ *limiting.MemoryConsumptionTracker) ([]types.SeriesMetadata, error) {
		dst := dstLabelOp.GetValue()
		if !model.LabelName(dst).IsValid() {
			return nil, fmt.Errorf("invalid destination label name in label_join(): %s", dst)
		}
		separator := separatorOp.GetValue()
		srcLabels := make([]string, len(srcLabelOps))
		for i, op := range srcLabelOps {
			src := op.GetValue()
			if !model.LabelName(src).IsValid() {
				return nil, fmt.Errorf("invalid source label name in label_join(): %s", dst)
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
			seriesMetadata[i].Labels = lb.Labels()
		}

		return seriesMetadata, nil
	}
}

func LabelReplaceFactory(dstLabelOp, replacementOp, srcLabelOp, regexOp types.StringOperator) SeriesMetadataFunction {
	return func(seriesMetadata []types.SeriesMetadata, _ *limiting.MemoryConsumptionTracker) ([]types.SeriesMetadata, error) {
		regexStr := regexOp.GetValue()
		regex, err := regexp.Compile("^(?s:" + regexStr + ")$")
		if err != nil {
			return nil, fmt.Errorf("invalid regular expression in label_replace(): %s", regexStr)
		}
		dst := dstLabelOp.GetValue()
		// TODO(jhesketh): Use UTF-8 validation (model.LabelName(src).IsValid()) when https://github.com/prometheus/prometheus/pull/15974 is vendored in.
		if !model.LabelNameRE.MatchString(dst) {
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
				seriesMetadata[i].Labels = lb.Labels()
			}
		}

		return seriesMetadata, nil
	}
}
