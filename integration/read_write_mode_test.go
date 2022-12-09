// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker
// +build requires_docker

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/grafana/mimir/integration/e2emimir"
)

func TestReadWriteModeQueryingIngester(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	c, _, _ := startReadWriteModeCluster(t, s)

	// Push some data to the cluster.
	now := time.Now()
	series, expectedVector, expectedMatrix := generateSeries("test_series_1", now, prompb.Label{Name: "foo", Value: "bar"})

	res, err := c.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Verify we can read the data we just pushed, both with an instant query and a range query.
	queryResult, err := c.Query("test_series_1", now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, queryResult.Type())
	require.Equal(t, expectedVector, queryResult.(model.Vector))

	rangeResult, err := c.QueryRange("test_series_1", now.Add(-5*time.Minute), now, 15*time.Second)
	require.NoError(t, err)
	require.Equal(t, model.ValMatrix, rangeResult.Type())
	require.Equal(t, expectedMatrix, rangeResult.(model.Matrix))

	// Verify we can retrieve the labels we just pushed.
	labelValues, err := c.LabelValues("foo", prometheusMinTime, prometheusMaxTime, nil)
	require.NoError(t, err)
	require.Equal(t, model.LabelValues{"bar"}, labelValues)

	labelNames, err := c.LabelNames(prometheusMinTime, prometheusMaxTime)
	require.NoError(t, err)
	require.Equal(t, []string{"__name__", "foo"}, labelNames)
}

func TestReadWriteModeQueryingStoreGateway(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	c, writeInstance, backendInstance := startReadWriteModeCluster(t, s, BlocksStorageFlags(), map[string]string{
		// Frequently compact and ship blocks to storage so we can query them through the store gateway.
		"-blocks-storage.tsdb.block-ranges-period":          "2s",
		"-blocks-storage.tsdb.ship-interval":                "1s",
		"-blocks-storage.tsdb.retention-period":             "3s",
		"-blocks-storage.tsdb.head-compaction-idle-timeout": "1s",
	})

	// Push some data to the cluster.
	now := time.Now()
	series, expectedVector, expectedMatrix := generateSeries("test_series_1", now, prompb.Label{Name: "foo", Value: "bar"})

	res, err := c.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Wait until the TSDB head is shipped to storage, removed from the ingester in the write instance, and loaded by the
	// store-gateway in the backend instance to ensure we're querying the store-gateway (and not the ingester).
	require.NoError(t, writeInstance.WaitSumMetrics(e2e.GreaterOrEqual(1), "cortex_ingester_shipper_uploads_total"))
	require.NoError(t, writeInstance.WaitSumMetrics(e2e.Equals(0), "cortex_ingester_memory_series"))
	require.NoError(t, backendInstance.WaitSumMetrics(e2e.GreaterOrEqual(1), "cortex_bucket_store_blocks_loaded"))

	// Verify we can read the data we just pushed, both with an instant query and a range query.
	queryResult, err := c.Query("test_series_1", now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, queryResult.Type())
	require.Equal(t, expectedVector, queryResult.(model.Vector))

	rangeResult, err := c.QueryRange("test_series_1", now.Add(-5*time.Minute), now, 15*time.Second)
	require.NoError(t, err)
	require.Equal(t, model.ValMatrix, rangeResult.Type())
	require.Equal(t, expectedMatrix, rangeResult.(model.Matrix))

	// Verify we can retrieve the labels we just pushed.
	labelValues, err := c.LabelValues("foo", prometheusMinTime, prometheusMaxTime, nil)
	require.NoError(t, err)
	require.Equal(t, model.LabelValues{"bar"}, labelValues)

	labelNames, err := c.LabelNames(prometheusMinTime, prometheusMaxTime)
	require.NoError(t, err)
	require.Equal(t, []string{"__name__", "foo"}, labelNames)
}

func TestReadWriteModeRecordingRule(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	c, _, backendInstance := startReadWriteModeCluster(
		t,
		s,
		map[string]string{
			// Evaluate rules often and with no delay, so that we don't need to wait for metrics to show up.
			"-ruler.evaluation-interval":       "2s",
			"-ruler.poll-interval":             "2s",
			"-ruler.evaluation-delay-duration": "0",
		},
	)

	// Push data that should be captured by the recording rule
	pushTime := time.Now()
	series, _, _ := generateSeries("test_series", pushTime, prompb.Label{Name: "foo", Value: "bar"})

	res, err := c.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Create recording rule
	// (we create the rule after pushing the data to avoid race conditions around pushing the data and evaluating the rule -
	// Mimir guarantees that previously pushed data will be captured by the recording rule evaluation)
	record := yaml.Node{}
	testRuleName := "test_rule"
	record.SetString(testRuleName)

	expr := yaml.Node{}
	expr.SetString("sum(test_series)")

	ruleGroup := rulefmt.RuleGroup{
		Name:     "test_rule_group",
		Interval: 1,
		Rules: []rulefmt.RuleNode{
			{
				Record: record,
				Expr:   expr,
			},
		},
	}

	require.NoError(t, c.SetRuleGroup(ruleGroup, "test_rule_group_namespace"))

	// Wait for recording rule to evaluate
	require.NoError(t, backendInstance.WaitSumMetricsWithOptions(e2e.GreaterOrEqual(1), []string{"cortex_prometheus_rule_evaluations_total"}, e2e.WaitMissingMetrics))

	// Verify recorded series is as expected
	queryTime := time.Now()
	queryResult, err := c.Query(testRuleName, queryTime)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, queryResult.Type())

	expectedVector := model.Vector{
		&model.Sample{
			Metric: model.Metric{
				labels.MetricName: model.LabelValue(testRuleName),
			},
			Value:     model.SampleValue(series[0].Samples[0].Value),
			Timestamp: model.Time(e2e.TimeToMilliseconds(queryTime)),
		},
	}

	require.Equal(t, expectedVector, queryResult.(model.Vector))
}

func TestReadWriteModeAlertingRule(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	c, _, backendInstance := startReadWriteModeCluster(
		t,
		s,
		map[string]string{
			// Evaluate rules often and with no delay, so that we don't need to wait for metrics or alerts to show up.
			"-ruler.evaluation-interval":       "2s",
			"-ruler.poll-interval":             "2s",
			"-ruler.evaluation-delay-duration": "0",
			"-ruler.resend-delay":              "2s",
		},
	)

	// Set up alertmanager config for tenant
	alertmanagerConfig := `
route:
  receiver: test-receiver
receivers:
  - name: test-receiver
`
	require.NoError(t, c.SetAlertmanagerConfig(context.Background(), alertmanagerConfig, map[string]string{}))

	// Create alerting rule
	alert := yaml.Node{}
	testAlertName := "test_alert"
	alert.SetString(testAlertName)

	expr := yaml.Node{}
	expr.SetString("sum(test_series) > 0")

	ruleGroup := rulefmt.RuleGroup{
		Name:     "test_rule_group",
		Interval: 1,
		Rules: []rulefmt.RuleNode{
			{
				Alert: alert,
				Expr:  expr,
				For:   model.Duration(1 * time.Second),
			},
		},
	}

	require.NoError(t, c.SetRuleGroup(ruleGroup, "test_rule_group_namespace"))

	// Push data that should trigger the alerting rule
	pushTime := time.Now()
	series, _, _ := generateSeries("test_series", pushTime, prompb.Label{Name: "foo", Value: "bar"})

	res, err := c.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Verify alert is firing
	require.NoError(t, backendInstance.WaitSumMetricsWithOptions(e2e.GreaterOrEqual(1), []string{"cortex_alertmanager_alerts_received_total"}, e2e.WaitMissingMetrics))

	alerts, err := c.GetAlertsV1(context.Background())
	require.NoError(t, err)
	require.Len(t, alerts, 1)
	require.Equal(t, testAlertName, alerts[0].Name())
}

func startReadWriteModeCluster(t *testing.T, s *e2e.Scenario, extraFlags ...map[string]string) (c *e2emimir.Client, writeInstance *e2emimir.MimirService, backendInstance *e2emimir.MimirService) {
	minio := e2edb.NewMinio(9000, mimirBucketName)
	require.NoError(t, s.StartAndWaitReady(minio))

	flagSets := []map[string]string{
		CommonStorageBackendFlags(),
		{
			"-memberlist.join": "mimir-backend-1",
		},
	}

	flagSets = append(flagSets, extraFlags...)
	commonFlags := mergeFlags(flagSets...)
	backendFlags := mergeFlags(commonFlags, map[string]string{"-ruler.alertmanager-url": "http://localhost:8080/alertmanager"})

	readInstance := e2emimir.NewReadInstance("mimir-read-1", commonFlags)
	writeInstance = e2emimir.NewWriteInstance("mimir-write-1", commonFlags)
	backendInstance = e2emimir.NewBackendInstance("mimir-backend-1", backendFlags)
	require.NoError(t, s.StartAndWaitReady(readInstance, writeInstance, backendInstance))

	c, err := e2emimir.NewClient(writeInstance.HTTPEndpoint(), readInstance.HTTPEndpoint(), backendInstance.HTTPEndpoint(), backendInstance.HTTPEndpoint(), "user-1")
	require.NoError(t, err)

	// Wait for the ingester to join the ring and become active - this prevents "empty ring" errors later when we try to query data.
	require.NoError(t, readInstance.WaitSumMetricsWithOptions(
		e2e.Equals(1),
		[]string{"cortex_ring_members"},
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"), labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE")),
	))

	return
}
