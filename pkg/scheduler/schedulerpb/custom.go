// SPDX-License-Identifier: AGPL-3.0-only

package schedulerpb

import (
	"github.com/pkg/errors"
)

var (
	ErrSchedulerIsNotRunning = errors.New("scheduler is not running")
)

func MetadataSliceToMap(m []MetadataItem) map[string][]string {
	metadataMap := make(map[string][]string, len(m))

	for _, item := range m {
		metadataMap[item.Key] = item.Value
	}

	return metadataMap
}

func MapToMetadataSlice(m map[string][]string) []MetadataItem {
	metadataSlice := make([]MetadataItem, 0, len(m))

	for k, v := range m {
		metadataSlice = append(metadataSlice, MetadataItem{
			Key:   k,
			Value: v,
		})
	}

	return metadataSlice
}

type MetadataMapTracingCarrier map[string][]string

func (m MetadataMapTracingCarrier) Get(key string) string {
	if values := m[key]; len(values) > 0 {
		return values[0]
	}

	return ""
}

func (m MetadataMapTracingCarrier) Set(key, value string) {
	m[key] = []string{value}
}

func (m MetadataMapTracingCarrier) Keys() []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
