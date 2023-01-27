package kafka

import (
	"bytes"
	"errors"
	"sort"
	"strings"
	"unsafe"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/extract"
	"github.com/grafana/mimir/pkg/util/validation"
	"github.com/prometheus/prometheus/model/labels"
)

const userSep = '\xfe'
const dropped_labels_label = "__dropped_labels__"

func ComposeKafkaKey(buf, user []byte, lsetAdapter []mimirpb.LabelAdapter, rules validation.ForwardingRules) ([]byte, error) {
	lset := mimirpb.FromLabelAdaptersToLabels(lsetAdapter)
	metricName, err := extract.MetricNameFromLabels(lset)
	if err != nil {
		return nil, err
	}

	keyBuilder := bytes.NewBuffer(buf[:0])
	_, err = keyBuilder.Write(user)
	if err != nil {
		return nil, err
	}
	err = keyBuilder.WriteByte(userSep)
	if err != nil {
		return nil, err
	}

	lsetBuilder := labels.NewBuilder(lset)
	dropLabels := rules[metricName].DropLabels
	lsetBuilder.Del(dropLabels...)

	// Copy the labels to drop to not modify the slice passed into this function.
	dropped_labels := make([]string, 0, len(dropLabels))
	dropped_labels = append(dropped_labels, dropLabels...)
	sort.StringSlice(dropped_labels).Sort()
	lsetBuilder.Set(dropped_labels_label, strings.Join(dropped_labels, ","))

	keyBuilder.WriteString(lsetBuilder.Labels(nil).String())

	return keyBuilder.Bytes(), nil
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
