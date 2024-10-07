// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/value.go
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package functions

import (
	"fmt"
	"regexp"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func LabelReplaceFactory(dstLabelOp, replacementOp, srcLabelOp, regexOp types.StringOperator) SeriesMetadataFunction {
	return func(seriesMetadata []types.SeriesMetadata, _ *limiting.MemoryConsumptionTracker) ([]types.SeriesMetadata, error) {
		regexStr := regexOp.GetValue()
		regex, err := regexp.Compile("^(?s:" + regexStr + ")$")
		if err != nil {
			return nil, fmt.Errorf("invalid regular expression in label_replace(): %s", regexStr)
		}
		dst := dstLabelOp.GetValue()
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
