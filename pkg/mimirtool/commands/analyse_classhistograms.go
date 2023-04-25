// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/grafana/mimir/pkg/mimirtool/client"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"gopkg.in/alecthomas/kingpin.v2"
)

type ClassicHistogramsAnalyzeCommand struct {
	address       string
	username      string
	password      string
	readTimeout   time.Duration
	lookback      time.Duration
	scrapInterval time.Duration
	outputFile    string
	// concurrency int
}

func (cmd *ClassicHistogramsAnalyzeCommand)run(k *kingpin.ParseContext) error {
	api, err := client.NewAPI(cmd.address, cmd.username, cmd.password)
	if err != nil {
		return err
	}

	endTime := time.Now()
	startTime := endTime.Add(-cmd.lookback)

	bucketMetrics, err := cmd.queryBucketMetricNames(api, startTime, endTime)
	if err != nil {
		return err
	}
	fmt.Printf("Potential histogram metrics found: %d\n", len(bucketMetrics))

	for _, bucketMetricName := range bucketMetrics {
		basename, err := metricBasename(bucketMetricName)
		if err != nil {
			// just skip if something doesn't look like a good name
			continue
		}
		sumName := fmt.Sprintf("%s_sum", basename)
		vector, err := cmd.queryLabelSets(api, sumName, endTime, cmd.lookback)
		if err != nil {
			return err
		}

		for _, sample := range vector {
			// get labels to match
			lbs := model.LabelSet(sample.Metric)
			delete(lbs, labels.MetricName)
			seriesSel := seriesSelector(bucketMetricName, lbs)
			// estimate the step
			step := int64(math.Round(cmd.lookback.Seconds() / float64(sample.Value)))

			stats, err := cmd.queryBucketStatistics(api, seriesSel, startTime, endTime, time.Duration(step) * time.Second)
			if err != nil {
				return err
			}
			fmt.Printf("%s=%v\n", seriesSel, *stats)
		}
	}

	return nil
}

func (cmd *ClassicHistogramsAnalyzeCommand)queryBucketMetricNames(api v1.API, start, end time.Time) ([]string, error) {
	values, _, err := api.LabelValues(context.Background(), labels.MetricName, []string{"{__name__=~\".*_bucket\"}"}, start, end)
	if err != nil {
		return nil, err
	}
	result := []string{}
	for _, v := range values {
		result = append(result, string(v))
	}
	return result, nil
}

// Query the related count_over_time(*_sum[lookback]) series to double check that metricName is a
// histogram. This keeps the result small (avoids buckets) and the count gives scrape interval
// when dividing lookback with it.
func (cmd *ClassicHistogramsAnalyzeCommand)queryLabelSets(api v1.API, metricName string, end time.Time, lookback time.Duration) (model.Vector, error) {
	query := fmt.Sprintf("count_over_time(%s{}[%s])", metricName, lookback.String())

	values, _, err := api.Query(context.Background(), query, end)
	if err != nil {
		return nil, err
	}

	vector, ok := values.(model.Vector)
	if !ok {
		return nil, fmt.Errorf("query for metrics resulted in non Vector")
	}
	return vector, nil
}

type statistics struct {
	available, max, min, avg int
	estimateScrapeInterval time.Duration
}

func (s statistics)String() string {
	return fmt.Sprintf("Bucket min/avg/max/avail@scrape: %d/%d/%d/%d@%v", s.min, s.avg, s.max, s.available, s.estimateScrapeInterval)
}

func (cmd *ClassicHistogramsAnalyzeCommand)queryBucketStatistics(api v1.API, query string, start, end time.Time, step time.Duration) (*statistics, error) {
	values, _, err := api.QueryRange(context.Background(), query, v1.Range{Start: start, End: end, Step: step})
	if err != nil {
		return nil, err
	}

	matrix, ok := values.(model.Matrix)
	if !ok {
		return nil, fmt.Errorf("query of buckets returned non Matrix")
	}

	stats := &statistics{
		available: len(matrix),
		min: math.MaxInt,
		estimateScrapeInterval: step,
	}

	if len(matrix) == 0 || len(matrix[0].Values)<2 {
		// not enough data
		return stats, nil
	}

	sumBuckets := 0
	// Assume the results are nicely aligned
	for idx := 1; idx<len(matrix[0].Values); idx++ {
		bucketChanged := 0
		for _, samples := range matrix {
			if samples.Values[idx-1].Timestamp != matrix[0].Values[idx-1].Timestamp {
				return nil, fmt.Errorf("matrix result is not time aligned")
			}
			if samples.Values[idx].Value != samples.Values[idx-1].Value {
				bucketChanged++
			}
		}
		sumBuckets+=bucketChanged
		if stats.min > bucketChanged {
			stats.min = bucketChanged
		}
		if stats.max < bucketChanged {
			stats.max = bucketChanged
		}
	}
	stats.avg = sumBuckets / (len(matrix[0].Values)-1)
	
	return stats, nil
}


func metricBasename(bucketMetricName string) (string, error) {
	if len(bucketMetricName) <= len("_bucket") {
		return "", fmt.Errorf("potential metric name too short: %s", bucketMetricName)
	}
	return bucketMetricName[:len(bucketMetricName)-len("_bucket")], nil
}

func seriesSelector(bucketMetricName string, lbs model.LabelSet) string {
	builder := strings.Builder{}
	builder.WriteString(bucketMetricName)
	builder.WriteRune('{')
	first := true
	for l, v := range lbs {
		if first {
			first = false
		} else {
			builder.WriteRune(',')
		}
		builder.WriteString(string(l))
		builder.WriteString("=\"")
		builder.WriteString(string(v))
		builder.WriteRune('"')
	}
	builder.WriteRune('}')

	return builder.String()
}