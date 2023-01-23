package kafka

import (
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/extract"
	"github.com/grafana/mimir/pkg/util/validation"
	"github.com/prometheus/prometheus/model/labels"
)

const userSep = '\xfe'

func ComposeKafkaKey(buf, user []byte, lsetAdapter []mimirpb.LabelAdapter, rules validation.ForwardingRules) ([]byte, error) {
	lset := mimirpb.FromLabelAdaptersToLabels(lsetAdapter)
	metricName, err := extract.MetricNameFromLabels(lset)
	if err != nil {
		return nil, err
	}

	metricRule := rules[metricName]
	aggregatedLset := make(labels.Labels, 0, len(lset))

OUTER_LABELS:
	for _, l := range lset {
		for _, dropLabel := range metricRule.DropLabels {
			if l.Name == dropLabel {
				continue OUTER_LABELS
			}
		}

		aggregatedLset = append(aggregatedLset, l)
	}

	key := append(user, userSep)
	return append(key, aggregatedLset.Bytes(buf)...), nil
}

func DecomposeKafkaKey(key []byte) ([]byte, []byte, error) {
	return nil, nil, nil
}
