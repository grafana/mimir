package mimirpb

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func prepareRequest() *WriteRequest {
	const numSeriesPerRequest = 100

	metrics := make([][]LabelAdapter, 0, numSeriesPerRequest)
	samples := make([]Sample, 0, 100)

	for i := 0; i < numSeriesPerRequest; i++ {
		metrics = append(metrics, []LabelAdapter{{Name: labels.MetricName, Value: "metric"}, {Name: "cardinality", Value: strconv.Itoa(i)}})
		samples = append(samples, Sample{Value: float64(i), TimestampMs: time.Now().UnixMilli()})
	}

	return ToWriteRequest(metrics, samples, nil, nil, API)
}

func TestSplitRequestWithWeirdSource(t *testing.T) {
	testCases := map[string]func(t *testing.T) []byte{
		"simple": func(t *testing.T) []byte {
			return marshal(t, prepareRequest())
		},

		"weird source": func(t *testing.T) []byte {
			wr := prepareRequest()
			wr.Source = WriteRequest_SourceEnum(123456)
			return marshal(t, wr)
		},

		"weird source, set skipLabelNameValidation": func(t *testing.T) []byte {
			wr := prepareRequest()
			wr.Source = WriteRequest_SourceEnum(123456)
			wr.SkipLabelNameValidation = true
			return marshal(t, wr)
		},

		"request with prepended fields": func(t *testing.T) []byte {
			wr := prepareRequest()
			wr.Source = WriteRequest_SourceEnum(123456)
			wr.SkipLabelNameValidation = true
			m := marshal(t, wr)

			raw := []byte(nil)
			raw = append(raw, toVarint(sourceFieldTag)...)
			raw = append(raw, toVarint(32516)...)

			raw = append(raw, toVarint(skipLabelNameValidationFieldTag)...)
			raw = append(raw, toVarint(0)...)

			raw = append(raw, m...)

			// Verify that entire "raw" message can be unmarshaled.
			wr.Reset()
			err := wr.Unmarshal(raw)
			require.NoError(t, err)

			// Check that last values are honored.
			require.Equal(t, WriteRequest_SourceEnum(123456), wr.Source) // from wr
			require.True(t, wr.SkipLabelNameValidation)                  // from wr

			return raw
		},

		"request with repeated appended fields": func(t *testing.T) []byte {
			wr := prepareRequest()
			wr.Source = WriteRequest_SourceEnum(123456)
			wr.SkipLabelNameValidation = true
			raw := marshal(t, wr)

			// Add another "source" field
			raw = append(raw, toVarint(sourceFieldTag)...)
			raw = append(raw, toVarint(32516)...)

			// Add new "skipLabelNameValidation" field
			raw = append(raw, toVarint(skipLabelNameValidationFieldTag)...)
			raw = append(raw, toVarint(0)...)

			// One more "source"
			raw = append(raw, toVarint(sourceFieldTag)...)
			raw = append(raw, toVarint(555)...)

			// One more "skipLabelNameValidation" field. Technically bools should only have 0 or 1 values, but gogoproto accepts non-zero value as true.
			raw = append(raw, toVarint(skipLabelNameValidationFieldTag)...)
			raw = append(raw, toVarint(5)...)

			// Verify that we can unmarshal this, and both source and skipLabelNameValidation are set to last value in the message.
			wr.Reset()
			err := wr.Unmarshal(raw)
			require.NoError(t, err)

			// Check that last values are honored.
			require.Equal(t, WriteRequest_SourceEnum(555), wr.Source) // last appended source value
			require.True(t, wr.SkipLabelNameValidation)               // last appended skipLabelNameValidation value

			return raw
		},
	}

	for name, f := range testCases {
		marshalled := f(t)
		size := len(marshalled)

		for _, maxSize := range []int{size / 10, size / 5, size / 2, size - maxExtraBytes, size, size + maxExtraBytes} {
			var expectedSubrequests int
			if size <= maxSize {
				expectedSubrequests = 1
			} else {
				expectedSubrequests = int(math.Ceil(float64(size) / float64(maxSize-maxExtraBytes)))
			}

			t.Run(fmt.Sprintf("%s: total size: %d, max size: %d, expected requests: %d", name, size, maxSize, expectedSubrequests), func(t *testing.T) {
				reqs, err := SplitWriteRequestRequest(marshalled, maxSize)
				require.NoError(t, err)
				require.Equal(t, len(reqs), expectedSubrequests)
				verifyRequests(t, reqs, marshalled, maxSize)
			})
		}
	}
}

func marshal(t *testing.T, wr *WriteRequest) []byte {
	m, err := wr.Marshal()
	require.NoError(t, err)
	return m
}

func verifyRequests(t *testing.T, reqs [][]byte, original []byte, maxSize int) {
	wr := &WriteRequest{}
	err := wr.Unmarshal(original)
	require.NoError(t, err)

	// combine all subrequests together into single request, and verify that it's the same as original
	combined := WriteRequest{
		Source:                  wr.Source,                  // checked separately
		SkipLabelNameValidation: wr.SkipLabelNameValidation, // checked separately
	}

	for ix := range reqs {
		require.LessOrEqual(t, len(reqs[ix]), maxSize)

		p := WriteRequest{}
		// Check that all requests can be parsed.
		err := p.Unmarshal(reqs[ix])
		require.NoError(t, err)

		// Verify that all parsed requests have same source and skipValidation fields as original
		require.Equal(t, p.Source, wr.Source)
		require.Equal(t, p.SkipLabelNameValidation, wr.SkipLabelNameValidation)

		combined.Timeseries = append(combined.Timeseries, p.Timeseries...)
		combined.Metadata = append(combined.Metadata, p.Metadata...)
	}

	// Ideally we would use combined.Equal(wr), but PreallocWriteRequest uses PreallocTimeseries and that failes to compare with Timeseries
	// Instead, we just marshal both values and check serialized form.
	combinedSerialized, err := combined.Marshal()
	require.NoError(t, err)
	origSerialized, err := wr.Marshal()
	require.NoError(t, err)
	require.Equal(t, origSerialized, combinedSerialized)
}

func toVarint(val uint64) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], val)
	return buf[:n]
}
