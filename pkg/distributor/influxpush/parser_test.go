// SPDX-License-Identifier: AGPL-3.0-only

package influxpush

import (
	"bytes"
	"context"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

const maxSize = 100 << 10

func TestParseInfluxLineReader(t *testing.T) {
	tests := []struct {
		name           string
		url            string
		data           string
		expectedResult []mimirpb.TimeSeries
	}{
		{
			name: "parse simple line",
			url:  "/",
			data: "measurement,t1=v1 f1=\"v2\" 1465839830100400200",
			expectedResult: []mimirpb.TimeSeries{
				{
					Labels: []mimirpb.LabelAdapter{
						{Name: "__mimir_source__", Value: "influx"},
						{Name: "__name__", Value: "measurement_f1"},
						{Name: "t1", Value: "v1"},
					},
					Samples: []mimirpb.Sample{{Value: 2, TimestampMs: 1465839830100}},
				},
			},
		},
		{
			name: "parse multiple tags",
			url:  "/",
			data: "measurement,t1=v1,t2=v2,t3=v3 f1=36 1465839830100400200",
			expectedResult: []mimirpb.TimeSeries{
				{
					Labels: []mimirpb.LabelAdapter{
						{Name: "__mimir_source__", Value: "influx"},
						{Name: "__name__", Value: "measurement_f1"},
						{Name: "t1", Value: "v1"},
						{Name: "t2", Value: "v2"},
						{Name: "t3", Value: "v3"},
					},
					Samples: []mimirpb.Sample{{Value: 36, TimestampMs: 1465839830100}},
				},
			},
		},
		{
			name: "parse multiple fields",
			url:  "/",
			data: "measurement,t1=v1 f1=3.0,f2=365,f3=0 1465839830100400200",
			expectedResult: []mimirpb.TimeSeries{
				{
					Labels: []mimirpb.LabelAdapter{
						{Name: "__mimir_source__", Value: "influx"},
						{Name: "__name__", Value: "measurement_f1"},
						{Name: "t1", Value: "v1"},
					},
					Samples: []mimirpb.Sample{{Value: 3, TimestampMs: 1465839830100}},
				},
				{
					Labels: []mimirpb.LabelAdapter{
						{Name: "__mimir_source__", Value: "influx"},
						{Name: "__name__", Value: "measurement_f2"},
						{Name: "t1", Value: "v1"},
					},
					Samples: []mimirpb.Sample{{Value: 365, TimestampMs: 1465839830100}},
				},
				{
					Labels: []mimirpb.LabelAdapter{
						{Name: "__mimir_source__", Value: "influx"},
						{Name: "__name__", Value: "measurement_f3"},
						{Name: "t1", Value: "v1"},
					},
					Samples: []mimirpb.Sample{{Value: 0, TimestampMs: 1465839830100}},
				},
			},
		},
		{
			name: "parse invalid char conversion",
			url:  "/",
			data: "*measurement,#t1?=v1 f1=0 1465839830100400200",
			expectedResult: []mimirpb.TimeSeries{
				{
					Labels: []mimirpb.LabelAdapter{
						{Name: "__mimir_source__", Value: "influx"},
						{Name: "__name__", Value: "_measurement_f1"},
						{Name: "_t1_", Value: "v1"},
					},
					Samples: []mimirpb.Sample{{Value: 0, TimestampMs: 1465839830100}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", tt.url, bytes.NewReader([]byte(tt.data)))

			timeSeries, _, err := ParseInfluxLineReader(context.Background(), req, maxSize)
			require.NoError(t, err)

			if len(timeSeries) > 1 {
				// sort the returned timeSeries results in guarantee expected order for comparison
				sort.Slice(timeSeries, func(i, j int) bool {
					return timeSeries[i].String() < timeSeries[j].String()
				})
			}
			for i := 1; i < len(timeSeries); i++ {
				assert.Equal(t, timeSeries[i].String(), tt.expectedResult[i].String())
			}
		})
	}
}

func TestParseInfluxInvalidInput(t *testing.T) {
	tests := []struct {
		name string
		url  string
		data string
	}{
		{
			name: "parse invalid precision",
			url:  "/write?precision=ss", // precision must be of type "ns", "us", "ms", "s"
			data: "measurement,t1=v1 f1=2 1465839830100400200",
		},
		{
			name: "parse invalid field input",
			url:  "/write",
			data: "measurement,t1=v1 f1= 1465839830100400200", // field value is missing
		},
		{
			name: "parse invalid tags",
			url:  "/write",
			data: "measurement,t1=v1,t2 f1=2 1465839830100400200", // field value is missing
		},
		{
			name: "parse field value invalid quotes",
			url:  "/write",
			data: "measurement,t1=v1 f1=v1 1465839830100400200", // string type field values require double quotes
		},
		{
			name: "parse missing field",
			url:  "/write",
			data: "measurement,t1=v1 1465839830100400200", // missing field
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", tt.url, bytes.NewReader([]byte(tt.data)))

			_, _, err := ParseInfluxLineReader(context.Background(), req, maxSize)
			require.Error(t, err)
		})
	}
}

func TestParseInfluxBatchReadCloser(t *testing.T) {
	req := httptest.NewRequest("POST", "/write", bytes.NewReader([]byte("m,t1=v1 f1=2 1465839830100400200")))
	req.Header.Add("Content-Encoding", "gzip")

	_, err := batchReadCloser(req.Body, "gzip", int64(maxSize))
	require.Error(t, err)
}
