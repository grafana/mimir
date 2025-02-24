// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/prompb"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/grafana/mimir/integration/e2emimir"
	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestReadWriteModeQueryingIngester(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	client, _ := startReadWriteModeCluster(t, s)

	runQueryingIngester(t, client, "test_series_1", generateFloatSeries)
	runQueryingIngester(t, client, "test_hseries_1", generateHistogramSeries)
}

func runQueryingIngester(t *testing.T, client *e2emimir.Client, seriesName string, genSeries generateSeriesFunc) {
	// Push some data to the cluster.
	now := time.Now()
	series, expectedVector, expectedMatrix := genSeries(seriesName, now, prompb.Label{Name: "foo", Value: "bar"})

	res, err := client.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Verify we can read the data we just pushed, both with an instant query and a range query.
	queryResult, err := client.Query(seriesName, now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, queryResult.Type())
	require.Equal(t, expectedVector, queryResult.(model.Vector))

	rangeResult, err := client.QueryRange(seriesName, now.Add(-5*time.Minute), now, 15*time.Second)
	require.NoError(t, err)
	require.Equal(t, model.ValMatrix, rangeResult.Type())
	require.Equal(t, expectedMatrix, rangeResult.(model.Matrix))

	// Verify we can retrieve the labels we just pushed.
	labelValues, err := client.LabelValues("foo", v1.MinTime, v1.MaxTime, nil)
	require.NoError(t, err)
	require.Equal(t, model.LabelValues{"bar"}, labelValues)

	labelNames, err := client.LabelNames(v1.MinTime, v1.MaxTime, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"__name__", "foo"}, labelNames)
}

func TestReadWriteModeQueryingStoreGateway(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	client, cluster := startReadWriteModeCluster(t, s, BlocksStorageFlags(), map[string]string{
		// Frequently compact and ship blocks to storage so we can query them through the store gateway.
		"-blocks-storage.tsdb.block-ranges-period":          "2s",
		"-blocks-storage.tsdb.ship-interval":                "1s",
		"-blocks-storage.tsdb.retention-period":             "3s",
		"-blocks-storage.tsdb.head-compaction-idle-timeout": "1s",
	})

	runQueryingStoreGateway(t, client, cluster, "test_series_1", generateFloatSeries)
	runQueryingStoreGateway(t, client, cluster, "test_hseries_1", generateHistogramSeries)
}

func runQueryingStoreGateway(t *testing.T, client *e2emimir.Client, cluster readWriteModeCluster, seriesName string, genSeries generateSeriesFunc) {
	// Push some data to the cluster.
	now := time.Now()
	series, expectedVector, expectedMatrix := genSeries(seriesName, now, prompb.Label{Name: "foo", Value: "bar"})

	res, err := client.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Wait until the TSDB head is shipped to storage, removed from the ingester in the write instance, and loaded by the
	// store-gateway in the backend instance to ensure we're querying the store-gateway (and not the ingester).
	require.NoError(t, cluster.writeInstance.WaitSumMetrics(e2e.GreaterOrEqual(1), "cortex_ingester_shipper_uploads_total"))
	require.NoError(t, cluster.writeInstance.WaitSumMetrics(e2e.Equals(0), "cortex_ingester_memory_series"))
	require.NoError(t, cluster.backendInstance.WaitSumMetrics(e2e.GreaterOrEqual(1), "cortex_bucket_store_blocks_loaded"))

	// Verify we can read the data we just pushed, both with an instant query and a range query.
	queryResult, err := client.Query(seriesName, now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, queryResult.Type())
	require.Equal(t, expectedVector, queryResult.(model.Vector))

	rangeResult, err := client.QueryRange(seriesName, now.Add(-5*time.Minute), now, 15*time.Second)
	require.NoError(t, err)
	require.Equal(t, model.ValMatrix, rangeResult.Type())
	require.Equal(t, expectedMatrix, rangeResult.(model.Matrix))

	// Verify we can retrieve the labels we just pushed.
	labelValues, err := client.LabelValues("foo", v1.MinTime, v1.MaxTime, nil)
	require.NoError(t, err)
	require.Equal(t, model.LabelValues{"bar"}, labelValues)

	labelNames, err := client.LabelNames(v1.MinTime, v1.MaxTime, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"__name__", "foo"}, labelNames)
}

func TestReadWriteModeRecordingRule(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	client, cluster := startReadWriteModeCluster(
		t,
		s,
		map[string]string{
			// Evaluate rules often and with no delay, so that we don't need to wait for metrics to show up.
			"-ruler.evaluation-interval":       "2s",
			"-ruler.poll-interval":             "2s",
			"-ruler.evaluation-delay-duration": "0",
		},
	)

	seriesNameFloat := "test_series"
	seriesNameHisto := "test_hseries"
	aggFunc := "sum"
	testRuleNameFloat := "test_rule"
	testRuleNameHisto := "test_hrule"

	seriesFloat := runRecordingRulePush(t, client, seriesNameFloat, generateFloatSeries)
	seriesHisto := runRecordingRulePush(t, client, seriesNameHisto, generateHistogramSeries)

	// Create recording rule
	// (we create the rule after pushing the data to avoid race conditions around pushing the data and evaluating the rule -
	// Mimir guarantees that previously pushed data will be captured by the recording rule evaluation)
	recordFloat := yaml.Node{}
	recordFloat.SetString(testRuleNameFloat)
	recordHisto := yaml.Node{}
	recordHisto.SetString(testRuleNameHisto)

	exprFloat := yaml.Node{}
	exprFloat.SetString(fmt.Sprintf("%s(%s)", aggFunc, seriesNameFloat))
	exprHisto := yaml.Node{}
	exprHisto.SetString(fmt.Sprintf("%s(%s)", aggFunc, seriesNameHisto))

	ruleGroup := rulefmt.RuleGroup{
		Name:     "test_rule_group",
		Interval: 1,
		Rules: []rulefmt.RuleNode{
			{
				Record: recordFloat,
				Expr:   exprFloat,
			},
			{
				Record: recordHisto,
				Expr:   exprHisto,
			},
		},
	}

	require.NoError(t, client.SetRuleGroup(ruleGroup, "test_rule_group_namespace"))

	// Wait for recording rule to evaluate
	require.NoError(t, cluster.backendInstance.WaitSumMetricsWithOptions(e2e.GreaterOrEqual(1), []string{"cortex_prometheus_rule_evaluations_total"}, e2e.WaitMissingMetrics))

	runRecordingRuleQuery(t, client, testRuleNameFloat, seriesFloat)
	runRecordingRuleQuery(t, client, testRuleNameHisto, seriesHisto)
}

func runRecordingRulePush(t *testing.T, client *e2emimir.Client, seriesName string, genSeries generateSeriesFunc) []prompb.TimeSeries {
	// Push data that should be captured by the recording rule
	pushTime := time.Now()
	series, _, _ := genSeries(seriesName, pushTime, prompb.Label{Name: "foo", Value: "bar"})

	res, err := client.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	return series
}

func runRecordingRuleQuery(t *testing.T, client *e2emimir.Client, testRuleName string, series []prompb.TimeSeries) {
	// Verify recorded series is as expected
	queryTime := time.Now()
	queryResult, err := client.Query(testRuleName, queryTime)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, queryResult.Type())

	metric := model.Metric{
		labels.MetricName: model.LabelValue(testRuleName),
	}
	var expectedVector model.Vector
	if len(series[0].Samples) > 0 {
		expectedVector = model.Vector{
			&model.Sample{
				Metric:    metric,
				Value:     model.SampleValue(series[0].Samples[0].Value),
				Timestamp: model.Time(e2e.TimeToMilliseconds(queryTime)),
			},
		}
	} else {
		expectedVector = model.Vector{
			&model.Sample{
				Metric:    metric,
				Histogram: mimirpb.FromHistogramToPromHistogram(series[0].Histograms[0].ToIntHistogram()),
				Timestamp: model.Time(e2e.TimeToMilliseconds(queryTime)),
			},
		}
	}

	require.Equal(t, expectedVector, queryResult.(model.Vector))
}

func TestReadWriteModeAlertingRule(t *testing.T) {
	runAlertingRule(t, "test_alert", "test_series", "sum", generateFloatSeries)
	runAlertingRule(t, "test_halert", "test_hseries", "histogram_sum", generateHistogramSeries)
}

func runAlertingRule(t *testing.T, testAlertName, seriesName, aggFunc string, genSeries generateSeriesFunc) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	client, cluster := startReadWriteModeCluster(
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
	require.NoError(t, client.SetAlertmanagerConfig(context.Background(), alertmanagerConfig, map[string]string{}))

	// Create alerting rule
	alert := yaml.Node{}
	alert.SetString(testAlertName)

	expr := yaml.Node{}
	expr.SetString(fmt.Sprintf("%s(%s) > 0", aggFunc, seriesName))

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

	require.NoError(t, client.SetRuleGroup(ruleGroup, "test_rule_group_namespace"))

	// Push data that should trigger the alerting rule
	pushTime := time.Now()
	series, _, _ := genSeries(seriesName, pushTime, prompb.Label{Name: "foo", Value: "bar"})

	res, err := client.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Verify alert is firing
	require.NoError(t, cluster.backendInstance.WaitSumMetricsWithOptions(e2e.GreaterOrEqual(1), []string{"cortex_alertmanager_alerts_received_total"}, e2e.WaitMissingMetrics))

	alerts, err := client.GetAlerts(context.Background())
	require.NoError(t, err)
	require.Len(t, alerts, 1)
	require.Equal(t, testAlertName, alerts[0].Name())
}

func TestReadWriteModeCompaction(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	client, cluster := startReadWriteModeCluster(t, s, BlocksStorageFlags(), map[string]string{
		// Frequently compact and ship blocks to storage so the compactor can compact them.
		"-blocks-storage.tsdb.block-ranges-period":          "2s",
		"-blocks-storage.tsdb.ship-interval":                "1s",
		"-blocks-storage.tsdb.retention-period":             "3s",
		"-blocks-storage.tsdb.head-compaction-idle-timeout": "1s",

		// Frequently cleanup old blocks.
		// While this doesn't test the compaction functionality of the compactor, it does verify that the compactor
		// is correctly configured and able to interact with storage, which is the intention of this test.
		"-compactor.blocks-retention-period": "5s",
	})

	runCompaction(t, client, cluster, "test_series_1", generateFloatSeries)
	runCompaction(t, client, cluster, "test_hseries_1", generateHistogramSeries)
}

func runCompaction(t *testing.T, client *e2emimir.Client, cluster readWriteModeCluster, seriesName string, genSeries generateSeriesFunc) {
	// Push some data to the cluster.
	series, _, _ := genSeries(seriesName, time.Now(), prompb.Label{Name: "foo", Value: "bar"})

	res, err := client.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Wait until the TSDB head is shipped to storage.
	require.NoError(t, cluster.writeInstance.WaitSumMetrics(e2e.GreaterOrEqual(1), "cortex_ingester_shipper_uploads_total"))

	// Wait until the compactor discovers and deletes the old block.
	require.NoError(t, cluster.backendInstance.WaitSumMetricsWithOptions(
		e2e.GreaterOrEqual(1),
		[]string{"cortex_compactor_blocks_marked_for_deletion_total"},
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "reason", "retention")),
	))
}

func startReadWriteModeCluster(t *testing.T, s *e2e.Scenario, extraFlags ...map[string]string) (*e2emimir.Client, readWriteModeCluster) {
	minio := e2edb.NewMinio(9000, mimirBucketName)
	require.NoError(t, s.StartAndWaitReady(minio))

	flagSets := []map[string]string{
		CommonStorageBackendFlags(),
		BlocksStorageFlags(),
		{
			"-memberlist.join": "mimir-backend-1",
		},
	}

	flagSets = append(flagSets, extraFlags...)
	commonFlags := mergeFlags(flagSets...)
	backendFlags := mergeFlags(commonFlags, map[string]string{"-ruler.alertmanager-url": "http://localhost:8080/alertmanager"})

	cluster := readWriteModeCluster{
		readInstance:    e2emimir.NewReadInstance("mimir-read-1", commonFlags),
		writeInstance:   e2emimir.NewWriteInstance("mimir-write-1", commonFlags),
		backendInstance: e2emimir.NewBackendInstance("mimir-backend-1", backendFlags),
	}
	require.NoError(t, s.StartAndWaitReady(cluster.readInstance, cluster.writeInstance, cluster.backendInstance))

	client, err := e2emimir.NewClient(cluster.writeInstance.HTTPEndpoint(), cluster.readInstance.HTTPEndpoint(), cluster.backendInstance.HTTPEndpoint(), cluster.backendInstance.HTTPEndpoint(), "user-1")
	require.NoError(t, err)

	// Wait for the ingester to join the ring and become active - this prevents "empty ring" errors later when we try to query data.
	require.NoError(t, cluster.readInstance.WaitSumMetricsWithOptions(
		e2e.Equals(1),
		[]string{"cortex_ring_members"},
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"), labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE")),
	))

	return client, cluster
}

type readWriteModeCluster struct {
	readInstance    *e2emimir.MimirService
	writeInstance   *e2emimir.MimirService
	backendInstance *e2emimir.MimirService
}
