package cassandra

import (
	"encoding/base64"

	"fmt"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
)

const (
	chunkTimeRangeKeyV1a = 1
	chunkTimeRangeKeyV1  = '1'
	chunkTimeRangeKeyV2  = '2'
	chunkTimeRangeKeyV3  = '3'
	chunkTimeRangeKeyV4  = '4'
	chunkTimeRangeKeyV5  = '5'

	// For v9 schema
	seriesRangeKeyV1      = '7'
	labelSeriesRangeKeyV1 = '8'
)

func decodeRangeKey(value []byte) [][]byte {
	components := make([][]byte, 0, 5)
	i, j := 0, 0
	for j < len(value) {
		if value[j] != 0 {
			j++
			continue
		}
		components = append(components, value[i:j])
		j++
		i = j
	}
	return components
}

func decodeBase64Value(bs []byte) (model.LabelValue, error) {
	decodedLen := base64.RawStdEncoding.DecodedLen(len(bs))
	decoded := make([]byte, decodedLen)
	if _, err := base64.RawStdEncoding.Decode(decoded, bs); err != nil {
		return "", err
	}
	return model.LabelValue(decoded), nil
}

// parseChunkTimeRangeValue returns the chunkID and labelValue for chunk time
// range values.
func parseChunkTimeRangeValue(rangeValue []byte, value []byte) (
	chunkID string, labelValue model.LabelValue, isSeriesID bool, err error,
) {
	components := decodeRangeKey(rangeValue)

	switch {
	case len(components) < 3:
		err = errors.Errorf("invalid chunk time range value: %x", rangeValue)
		return

	// v1 & v2 schema had three components - label name, label value and chunk ID.
	// No version number.
	case len(components) == 3:
		chunkID = string(components[2])
		labelValue = model.LabelValue(components[1])
		return

	case len(components[3]) == 1:
		switch components[3][0] {
		// v3 schema had four components - label name, label value, chunk ID and version.
		// "version" is 1 and label value is base64 encoded.
		// (older code wrote "version" as 1, not '1')
		case chunkTimeRangeKeyV1a, chunkTimeRangeKeyV1:
			chunkID = string(components[2])
			labelValue, err = decodeBase64Value(components[1])
			return

		// v4 schema wrote v3 range keys and a new range key - version 2,
		// with four components - <empty>, <empty>, chunk ID and version.
		case chunkTimeRangeKeyV2:
			chunkID = string(components[2])
			return

		// v5 schema version 3 range key is chunk end time, <empty>, chunk ID, version
		case chunkTimeRangeKeyV3:
			chunkID = string(components[2])
			return

		// v5 schema version 4 range key is chunk end time, label value, chunk ID, version
		case chunkTimeRangeKeyV4:
			chunkID = string(components[2])
			labelValue, err = decodeBase64Value(components[1])
			return

		// v6 schema added version 5 range keys, which have the label value written in
		// to the value, not the range key. So they are [chunk end time, <empty>, chunk ID, version].
		case chunkTimeRangeKeyV5:
			chunkID = string(components[2])
			labelValue = model.LabelValue(value)
			return

		// v9 schema actually return series IDs
		case seriesRangeKeyV1:
			chunkID = string(components[0])
			isSeriesID = true
			return

		case labelSeriesRangeKeyV1:
			chunkID = string(components[1])
			labelValue = model.LabelValue(value)
			isSeriesID = true
			return
		}
	}
	err = fmt.Errorf("unrecognised chunkTimeRangeKey version: %q", string(components[3]))
	return
}
