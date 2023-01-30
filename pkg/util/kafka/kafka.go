package kafka

import (
	"errors"
	"strings"
	"unsafe"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/extract"
	"github.com/grafana/mimir/pkg/util/validation"
	"github.com/prometheus/common/model"
)

const userSep = '\xfe'
const dropped_labels_label = "__dropped_labels__"

func ComposeKafkaKey(buf, user []byte, lsetAdapter []mimirpb.LabelAdapter, rules validation.ForwardingRules) ([]byte, error) {
	metricName, err := extract.MetricNameFromLabels(mimirpb.FromLabelAdaptersToLabels(lsetAdapter))
	if err != nil {
		return nil, err
	}

	rule := rules[metricName]
	dropLabelsMap := rule.DropLabelsMap()
	dropLabelsValue := rule.DropLabelsValue()

	buf = buf[:0]
	buf = append(buf, user...)
	buf = append(buf, userSep)
	buf = append(buf, metricName...)
	buf = append(buf, '{')

	var prevAppended string
	nextAppendCandidate := ""
	nextAppendCandidateVal := ""

	first := true
	for {
		// Check which label to apppend next.
		// The dropped labels label should be treated the same like the other labels in lsetAdapter.

		if dropped_labels_label > prevAppended && (dropped_labels_label < nextAppendCandidate || nextAppendCandidate == "") {
			nextAppendCandidate = dropped_labels_label
			nextAppendCandidateVal = dropLabelsValue
		}
		for _, l := range lsetAdapter {
			if l.Name == model.MetricNameLabel {
				continue
			}
			if _, ok := dropLabelsMap[l.Name]; ok {
				continue
			}
			if l.Name > prevAppended && (l.Name < nextAppendCandidate || nextAppendCandidate == "") {
				nextAppendCandidate = l.Name
				nextAppendCandidateVal = l.Value
			}
		}

		if nextAppendCandidate == "" {
			// We have appended all labels.
			break
		}

		if !first {
			buf = append(buf, ',')
		}

		buf = append(buf, nextAppendCandidate...)
		buf = append(buf, '=')
		buf = append(buf, '"')
		buf = append(buf, nextAppendCandidateVal...)
		buf = append(buf, '"')

		prevAppended = nextAppendCandidate
		nextAppendCandidate = ""
		first = false
	}

	buf = append(buf, '}')
	return buf, nil

}

func DecomposeKafkaKey(key []byte) (string, string, error) {
	keyStr := yoloString(key)
	sepIdx := strings.IndexByte(keyStr, userSep)
	if sepIdx < 0 {
		return "", "", errors.New("invalid key: no user separator: " + keyStr)
	}

	return keyStr[:sepIdx], keyStr[sepIdx+1:], nil
}

func yoloString(buf []byte) string {
	return *((*string)(unsafe.Pointer(&buf)))
}
