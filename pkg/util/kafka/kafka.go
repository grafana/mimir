package kafka

import (
	"bytes"
	"errors"
	"strings"
	"unsafe"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/extract"
	"github.com/grafana/mimir/pkg/util/validation"
	"github.com/prometheus/common/model"
)

const userSep = '\xfe'

func ComposeKafkaKey(buf, user []byte, lsetAdapter []mimirpb.LabelAdapter, rules validation.ForwardingRules) ([]byte, error) {
	lset := mimirpb.FromLabelAdaptersToLabels(lsetAdapter)
	metricName, err := extract.MetricNameFromLabels(lset)
	if err != nil {
		return nil, err
	}

	builder := bytes.NewBuffer(buf[:0])
	_, err = builder.Write(user)
	if err != nil {
		return nil, err
	}
	err = builder.WriteByte(userSep)
	if err != nil {
		return nil, err
	}
	_, err = builder.WriteString(metricName)
	if err != nil {
		return nil, err
	}
	err = builder.WriteByte('{')
	if err != nil {
		return nil, err
	}

	metricRule := rules[metricName]

	firstLabel := true
OUTER_LABELS:
	for _, l := range lset {
		if l.Name == model.MetricNameLabel {
			continue OUTER_LABELS
		}
		for _, dropLabel := range metricRule.DropLabels {
			if l.Name == dropLabel {
				continue OUTER_LABELS
			}
		}

		if !firstLabel {
			builder.WriteByte(',')
		} else {
			firstLabel = false
		}

		_, err = builder.WriteString(l.Name)
		if err != nil {
			return nil, err
		}
		err = builder.WriteByte('=')
		if err != nil {
			return nil, err
		}
		err = builder.WriteByte('"')
		if err != nil {
			return nil, err
		}
		_, err = builder.WriteString(l.Value)
		if err != nil {
			return nil, err
		}
		err = builder.WriteByte('"')
		if err != nil {
			return nil, err
		}
	}
	err = builder.WriteByte('}')
	if err != nil {
		return nil, err
	}

	return builder.Bytes(), nil
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
