// SPDX-License-Identifier: AGPL-3.0-only

package aws

import (
	"flag"
	"time"
)

type MetricsAutoScalingConfig struct {
	URL              string  `yaml:"url"`                   // URL to contact Prometheus store on
	TargetQueueLen   int64   `yaml:"target_queue_length"`   // Queue length above which we will scale up capacity
	ScaleUpFactor    float64 `yaml:"scale_up_factor"`       // Scale up capacity by this multiple
	MinThrottling    float64 `yaml:"ignore_throttle_below"` // Ignore throttling below this level
	QueueLengthQuery string  `yaml:"queue_length_query"`    // Promql query to fetch ingester queue length
	ThrottleQuery    string  `yaml:"write_throttle_query"`  // Promql query to fetch throttle rate per table
	UsageQuery       string  `yaml:"write_usage_query"`     // Promql query to fetch write capacity usage per table
	ReadUsageQuery   string  `yaml:"read_usage_query"`      // Promql query to fetch read usage per table
	ReadErrorQuery   string  `yaml:"read_error_query"`      // Promql query to fetch read errors per table
}

const (
	cachePromDataFor          = 30 * time.Second
	queueObservationPeriod    = 2 * time.Minute
	targetScaledown           = 0.1 // consider scaling down if queue smaller than this times target
	targetMax                 = 10  // always scale up if queue bigger than this times target
	throttleFractionScaledown = 0.1
	minUsageForScaledown      = 100 // only scale down if usage is > this DynamoDB units/sec

	// fetch Ingester queue length
	// average the queue length over 2 minutes to avoid aliasing with the 1-minute flush period
	defaultQueueLenQuery = `sum(avg_over_time(cortex_ingester_flush_queue_length{job="cortex/ingester"}[2m]))`
	// fetch write throttle rate per DynamoDB table
	defaultThrottleRateQuery = `sum(rate(cortex_dynamo_throttled_total{operation="DynamoDB.BatchWriteItem"}[1m])) by (table) > 0`
	// fetch write capacity usage per DynamoDB table
	// use the rate over 15 minutes so we take a broad average
	defaultUsageQuery = `sum(rate(cortex_dynamo_consumed_capacity_total{operation="DynamoDB.BatchWriteItem"}[15m])) by (table) > 0`
	// use the read rate over 1hr so we take a broad average
	defaultReadUsageQuery = `sum(rate(cortex_dynamo_consumed_capacity_total{operation="DynamoDB.QueryPages"}[1h])) by (table) > 0`
	// fetch read error rate per DynamoDB table
	defaultReadErrorQuery = `sum(increase(cortex_dynamo_failures_total{operation="DynamoDB.QueryPages",error="ProvisionedThroughputExceededException"}[1m])) by (table) > 0`
)

func (cfg *MetricsAutoScalingConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.URL, "metrics.url", "", "Use metrics-based autoscaling, via this query URL")
	f.Int64Var(&cfg.TargetQueueLen, "metrics.target-queue-length", 100000, "Queue length above which we will scale up capacity")
	f.Float64Var(&cfg.ScaleUpFactor, "metrics.scale-up-factor", 1.3, "Scale up capacity by this multiple")
	f.Float64Var(&cfg.MinThrottling, "metrics.ignore-throttle-below", 1, "Ignore throttling below this level (rate per second)")
	f.StringVar(&cfg.QueueLengthQuery, "metrics.queue-length-query", defaultQueueLenQuery, "query to fetch ingester queue length")
	f.StringVar(&cfg.ThrottleQuery, "metrics.write-throttle-query", defaultThrottleRateQuery, "query to fetch throttle rates per table")
	f.StringVar(&cfg.UsageQuery, "metrics.usage-query", defaultUsageQuery, "query to fetch write capacity usage per table")
	f.StringVar(&cfg.ReadUsageQuery, "metrics.read-usage-query", defaultReadUsageQuery, "query to fetch read capacity usage per table")
	f.StringVar(&cfg.ReadErrorQuery, "metrics.read-error-query", defaultReadErrorQuery, "query to fetch read errors per table")
}
