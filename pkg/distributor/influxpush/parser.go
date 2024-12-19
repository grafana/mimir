// SPDX-License-Identifier: AGPL-3.0-only

package influxpush

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"time"
	"unsafe"

	io2 "github.com/influxdata/influxdb/v2/kit/io"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util"
)

const internalLabel = "__mimir_source__"

// ParseInfluxLineReader parses a Influx Line Protocol request from an io.Reader.
func ParseInfluxLineReader(_ context.Context, r *http.Request, maxSize int) ([]mimirpb.PreallocTimeseries, int, error) {
	qp := r.URL.Query()
	precision := qp.Get("precision")
	if precision == "" {
		precision = "ns"
	}

	if !models.ValidPrecision(precision) {
		return nil, 0, fmt.Errorf("precision supplied is not valid: %s", precision)
	}

	encoding := r.Header.Get("Content-Encoding")
	reader, err := batchReadCloser(r.Body, encoding, int64(maxSize))
	if err != nil {
		return nil, 0, fmt.Errorf("gzip compression error: %w", err)
	}
	data, err := io.ReadAll(reader)
	dataLen := len(data) // In case if something is read despite an error.
	if err != nil {
		return nil, dataLen, fmt.Errorf("can't read body: %s", err)
	}

	err = reader.Close()
	if err != nil {
		return nil, dataLen, fmt.Errorf("problem reading body: %s", err)
	}

	points, err := models.ParsePointsWithPrecision(data, time.Now().UTC(), precision)
	if err != nil {
		return nil, dataLen, fmt.Errorf("can't parse points: %s", err)
	}
	ts, err := writeRequestFromInfluxPoints(points)
	return ts, dataLen, err
}

func writeRequestFromInfluxPoints(points []models.Point) ([]mimirpb.PreallocTimeseries, error) {
	// Technically the same series should not be repeated. We should put all the samples for
	// a series in single client.Timeseries. Having said that doing it is not very optimal and the
	// occurrence of multiple timestamps for the same series is rare. Only reason I see it happening is
	// for backfilling and this is not the API for that. Keeping that in mind, we are going to create a new
	// client.Timeseries for each sample.

	returnTs := mimirpb.PreallocTimeseriesSliceFromPool()[:0]
	if cap(returnTs) < len(points) {
		returnTs = make([]mimirpb.PreallocTimeseries, 0, len(points))
	}
	for _, pt := range points {
		var err error
		returnTs, err = influxPointToTimeseries(pt, returnTs)
		if err != nil {
			return nil, err
		}
	}

	return returnTs, nil
}

// Points to Prometheus is heavily inspired from https://github.com/prometheus/influxdb_exporter/blob/a1dc16ad596a990d8854545ea39a57a99a3c7c43/main.go#L148-L211
func influxPointToTimeseries(pt models.Point, returnTs []mimirpb.PreallocTimeseries) ([]mimirpb.PreallocTimeseries, error) {

	fields, err := pt.Fields()
	if err != nil {
		return returnTs, fmt.Errorf("can't get fields from point: %w", err)
	}
	for field, v := range fields {
		var value float64
		switch v := v.(type) {
		case float64:
			value = v
		case int64:
			value = float64(v)
		case bool:
			if v {
				value = 1
			} else {
				value = 0
			}
		default:
			continue
		}

		name := string(pt.Name()) + "_" + field
		if field == "value" {
			name = string(pt.Name())
		}
		replaceInvalidChars(&name)

		tags := pt.Tags()
		lbls := make([]mimirpb.LabelAdapter, 0, len(tags)+2) // An additional one for __name__, and one for internal label
		lbls = append(lbls, mimirpb.LabelAdapter{
			Name:  labels.MetricName,
			Value: name,
		})
		lbls = append(lbls, mimirpb.LabelAdapter{
			Name:  internalLabel, // An internal label for tracking active series
			Value: "influx",
		})
		for _, tag := range tags {
			key := string(tag.Key)
			if key == "__name__" || key == internalLabel {
				continue
			}
			replaceInvalidChars(&key)
			lbls = append(lbls, mimirpb.LabelAdapter{
				Name:  key,
				Value: yoloString(tag.Value),
			})
		}
		sort.Slice(lbls, func(i, j int) bool {
			return lbls[i].Name < lbls[j].Name
		})

		ts := mimirpb.TimeSeries{
			Labels: lbls,
			Samples: []mimirpb.Sample{{
				TimestampMs: util.TimeToMillis(pt.Time()),
				Value:       value,
			}},
		}

		returnTs = append(returnTs, mimirpb.PreallocTimeseries{TimeSeries: &ts})
	}

	return returnTs, nil
}

// analog of invalidChars = regexp.MustCompile("[^a-zA-Z0-9_]")
func replaceInvalidChars(in *string) {
	bSlice := []byte(*in)
	for bIndex, b := range bSlice {
		if !((b >= 'a' && b <= 'z') || // a-z
			(b >= 'A' && b <= 'Z') || // A-Z
			(b >= '0' && b <= '9') || // 0-9
			b == '_') { // _
			bSlice[bIndex] = '_'
		}
	}

	// prefix with _ if first char is 0-9
	if len(bSlice) > 0 && bSlice[0] >= '0' && bSlice[0] <= '9' {
		*in = "_" + string(bSlice)
	} else {
		*in = string(bSlice)
	}
}

// batchReadCloser (potentially) wraps an io.ReadCloser in Gzip
// decompression and limits the reading to a specific number of bytes.
func batchReadCloser(rc io.ReadCloser, encoding string, maxBatchSizeBytes int64) (io.ReadCloser, error) {
	switch encoding {
	case "gzip", "x-gzip":
		var err error
		rc, err = gzip.NewReader(rc)
		if err != nil {
			return nil, err
		}
	}
	if maxBatchSizeBytes > 0 {
		rc = io2.NewLimitedReadCloser(rc, maxBatchSizeBytes)
	}
	return rc, nil
}

// yoloString to create strings that are guaranteed not to be referenced again.
func yoloString(b []byte) string {
	return *((*string)(unsafe.Pointer(&b)))
}
