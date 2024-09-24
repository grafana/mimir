// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/ruler_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
//go:build requires_docker

package integration

import (
	"context"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/tenant"
	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/grafana/mimir/integration/ca"
	"github.com/grafana/mimir/integration/e2emimir"
	"github.com/grafana/mimir/pkg/querier/api"
	mimir_ruler "github.com/grafana/mimir/pkg/ruler"
)

func TestRulerAPI(t *testing.T) {
	var (
		namespaceOne = "test_/encoded_+namespace/?"
		namespaceTwo = "test_/encoded_+namespace/?/two"
		ruleGroup    = createTestRuleGroup()
	)

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, mimirBucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Configure the ruler.
	rulerFlags := mergeFlags(CommonStorageBackendFlags(), RulerFlags(), BlocksStorageFlags())

	// Start Mimir components.
	ruler := e2emimir.NewRuler("ruler", consul.NetworkHTTPEndpoint(), rulerFlags)
	require.NoError(t, s.StartAndWaitReady(ruler))

	// Create a client with the ruler address configured
	c, err := e2emimir.NewClient("", "", "", ruler.HTTPEndpoint(), "user-1")
	require.NoError(t, err)

	// Set the rule group into the ruler
	require.NoError(t, c.SetRuleGroup(ruleGroup, namespaceOne))

	// Wait until the user manager is created
	require.NoError(t, ruler.WaitSumMetrics(e2e.Equals(1), "cortex_ruler_managers_total"))

	// Check to ensure the rules running in the ruler match what was set
	_, rgs, err := c.GetRuleGroups()
	require.NoError(t, err)

	retrievedNamespace, exists := rgs[namespaceOne]
	require.True(t, exists)
	require.Len(t, retrievedNamespace, 1)
	require.Equal(t, retrievedNamespace[0].Name, ruleGroup.Name)

	// Add a second rule group with a similar namespace
	require.NoError(t, c.SetRuleGroup(ruleGroup, namespaceTwo))
	require.NoError(t, ruler.WaitSumMetrics(e2e.Equals(2), "cortex_prometheus_rule_group_rules"))

	// Check to ensure the rules running in the ruler match what was set
	_, rgs, err = c.GetRuleGroups()
	require.NoError(t, err)

	retrievedNamespace, exists = rgs[namespaceOne]
	require.True(t, exists)
	require.Len(t, retrievedNamespace, 1)
	require.Equal(t, retrievedNamespace[0].Name, ruleGroup.Name)

	retrievedNamespace, exists = rgs[namespaceTwo]
	require.True(t, exists)
	require.Len(t, retrievedNamespace, 1)
	require.Equal(t, retrievedNamespace[0].Name, ruleGroup.Name)

	// Test compression by inspecting the response Headers
	req, err := http.NewRequest("GET", fmt.Sprintf("http://%s/prometheus/config/v1/rules", ruler.HTTPEndpoint()), nil)
	require.NoError(t, err)

	req.Header.Set("X-Scope-OrgID", "user-1")
	req.Header.Set("Accept-Encoding", "gzip")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Execute HTTP request
	res, err := http.DefaultClient.Do(req.WithContext(ctx))
	require.NoError(t, err)

	defer res.Body.Close()
	// We assert on the Vary header as the minimum response size for enabling compression is 1500 bytes.
	// This is enough to know whenever the handler for compression is enabled or not.
	require.Equal(t, "Accept-Encoding", res.Header.Get("Vary"))

	// Delete the set rule groups
	require.NoError(t, c.DeleteRuleGroup(namespaceOne, ruleGroup.Name))
	require.NoError(t, c.DeleteRuleNamespace(namespaceTwo))

	// Get the rule group and ensure it returns a 404
	resp, err := c.GetRuleGroup(namespaceOne, ruleGroup.Name)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	// Wait until the users manager has been terminated
	require.NoError(t, ruler.WaitSumMetrics(e2e.Equals(0), "cortex_ruler_managers_total"))

	// Check to ensure the rule groups are no longer active
	_, groups, err := c.GetRuleGroups()
	require.NoError(t, err)
	require.Empty(t, groups)

	// Ensure no service-specific metrics prefix is used by the wrong service.
	assertServiceMetricsPrefixes(t, Ruler, ruler)
}

func TestRulerAPISingleBinary(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	namespace := "ns"
	user := "anonymous"

	// Start dependencies.
	minio := e2edb.NewMinio(9000, blocksBucketName)
	require.NoError(t, s.StartAndWaitReady(minio))

	flags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
		map[string]string{
			"-ruler-storage.local.directory": filepath.Join(e2e.ContainerSharedDir, "ruler_configs"),
			"-ruler.poll-interval":           "2s",
			"-ruler.rule-path":               filepath.Join(e2e.ContainerSharedDir, "rule_tmp/"),
		},
	)

	// Start Mimir components.
	require.NoError(t, copyFileToSharedDir(s, "docs/configurations/single-process-config-blocks.yaml", mimirConfigFile))
	require.NoError(t, writeFileToSharedDir(s, filepath.Join("ruler_configs", user, namespace), []byte(mimirRulerUserConfigYaml)))
	mimir := e2emimir.NewSingleBinary("mimir", flags, e2emimir.WithConfigFile(mimirConfigFile), e2emimir.WithPorts(9009, 9095))
	require.NoError(t, s.StartAndWaitReady(mimir))

	// Create a client with the ruler address configured
	c, err := e2emimir.NewClient("", "", "", mimir.HTTPEndpoint(), "")
	require.NoError(t, err)

	// Wait until the user manager is created
	require.NoError(t, mimir.WaitSumMetrics(e2e.Equals(1), "cortex_ruler_managers_total"))

	// Check to ensure the rules running in the mimir match what was set
	_, rgs, err := c.GetRuleGroups()
	require.NoError(t, err)

	retrievedNamespace, exists := rgs[namespace]
	require.True(t, exists)
	require.Len(t, retrievedNamespace, 1)
	require.Equal(t, retrievedNamespace[0].Name, "rule")

	// Check to make sure prometheus engine metrics are available for both engine types
	require.NoError(t, mimir.WaitSumMetricsWithOptions(e2e.Equals(0), []string{"prometheus_engine_queries"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "engine", "querier"))))

	require.NoError(t, mimir.WaitSumMetricsWithOptions(e2e.Equals(0), []string{"prometheus_engine_queries"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "engine", "ruler"))))

	// Test Cleanup and Restart

	// Stop the running mimir
	require.NoError(t, mimir.Stop())

	// Restart Mimir with identical configs
	mimirRestarted := e2emimir.NewSingleBinary("mimir-restarted", flags, e2emimir.WithConfigFile(mimirConfigFile), e2emimir.WithPorts(9009, 9095))
	require.NoError(t, s.StartAndWaitReady(mimirRestarted))

	// Wait until the user manager is created
	require.NoError(t, mimirRestarted.WaitSumMetrics(e2e.Equals(1), "cortex_ruler_managers_total"))
}

func TestRulerEvaluationDelay(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	namespace := "ns"
	user := "anonymous"

	evaluationDelay := time.Minute * 5

	// Start dependencies.
	minio := e2edb.NewMinio(9000, blocksBucketName)
	require.NoError(t, s.StartAndWaitReady(minio))

	flags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
		map[string]string{
			"-ruler-storage.local.directory":   filepath.Join(e2e.ContainerSharedDir, "ruler_configs"),
			"-ruler.poll-interval":             "2s",
			"-ruler.rule-path":                 filepath.Join(e2e.ContainerSharedDir, "rule_tmp/"),
			"-ruler.evaluation-delay-duration": evaluationDelay.String(),
		},
	)

	// Start Mimir components.
	require.NoError(t, copyFileToSharedDir(s, "docs/configurations/single-process-config-blocks.yaml", mimirConfigFile))
	require.NoError(t, writeFileToSharedDir(s, filepath.Join("ruler_configs", user, namespace), []byte(mimirRulerEvalStaleNanConfigYaml)))
	mimir := e2emimir.NewSingleBinary("mimir", flags, e2emimir.WithConfigFile(mimirConfigFile), e2emimir.WithPorts(9009, 9095))
	require.NoError(t, s.StartAndWaitReady(mimir))

	// Create a client with the ruler address configured
	c, err := e2emimir.NewClient(mimir.HTTPEndpoint(), mimir.HTTPEndpoint(), "", mimir.HTTPEndpoint(), "")
	require.NoError(t, err)

	now := time.Now()

	// Generate series that includes stale nans
	var samplesToSend = 10
	series := prompb.TimeSeries{
		Labels: []prompb.Label{
			{Name: "__name__", Value: "a_sometimes_stale_nan_series"},
			{Name: "instance", Value: "sometimes-stale"},
		},
	}
	series.Samples = make([]prompb.Sample, samplesToSend)
	posStale := 2

	// Create samples, that are delayed by the evaluation delay with increasing values.
	for pos := range series.Samples {
		series.Samples[pos].Timestamp = e2e.TimeToMilliseconds(now.Add(-evaluationDelay).Add(time.Duration(pos) * time.Second))
		series.Samples[pos].Value = float64(pos + 1)

		// insert staleness marker at the positions marked by posStale
		if pos == posStale {
			series.Samples[pos].Value = math.Float64frombits(value.StaleNaN)
		}
	}

	// Insert metrics
	res, err := c.Push([]prompb.TimeSeries{series})
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Get number of rule evaluations just after push
	ruleEvaluationsAfterPush, err := mimir.SumMetrics([]string{"cortex_prometheus_rule_evaluations_total"})
	require.NoError(t, err)

	// Wait until the rule is evaluated for the first time
	require.NoError(t, mimir.WaitSumMetrics(e2e.Greater(ruleEvaluationsAfterPush[0]), "cortex_prometheus_rule_evaluations_total"))

	// Query the timestamp of the latest result to ensure the evaluation is delayed
	result, err := c.Query("timestamp(stale_nan_eval)", now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())

	vector := result.(model.Vector)
	require.Equal(t, 1, vector.Len(), "expect one sample returned")

	// 290 seconds gives 10 seconds of slack between the rule evaluation and the query
	// to account for CI latency, but ensures the latest evaluation was in the past.
	var maxDiff int64 = 290_000
	require.GreaterOrEqual(t, e2e.TimeToMilliseconds(time.Now())-int64(vector[0].Value)*1000, maxDiff)

	// Wait until all the pushed samples have been evaluated by the rule. This
	// ensures that rule results are successfully written even after a
	// staleness period.
	require.NoError(t, mimir.WaitSumMetrics(e2e.Greater(ruleEvaluationsAfterPush[0]+float64(samplesToSend)), "cortex_prometheus_rule_evaluations_total"))

	// query all results to verify rules have been evaluated correctly
	t.Log("querying from ", now.Add(-evaluationDelay), "to", now)
	result, err = c.QueryRange("stale_nan_eval", now.Add(-evaluationDelay), now, time.Second)
	require.NoError(t, err)
	require.Equal(t, model.ValMatrix, result.Type())

	matrix := result.(model.Matrix)
	require.GreaterOrEqual(t, 1, matrix.Len(), "expect at least a series returned")

	// Iterate through the values recorded and ensure they exist as expected.
	inputPos := 0
	for _, m := range matrix {
		for _, v := range m.Values {
			// Skip values for stale positions
			if inputPos == posStale {
				inputPos++
			}

			expectedValue := model.SampleValue(2 * (inputPos + 1))
			assert.Equal(t, expectedValue, v.Value)
			t.Log(
				"expected value", expectedValue,
				"actual value", v.Value,
				"actual timestamp", v.Timestamp,
				"expected timestamp", now.Add(-evaluationDelay).Add(time.Duration(inputPos)*time.Second),
			)

			// Look for next value
			inputPos++

			// We have found all input values
			if inputPos >= len(series.Samples) {
				break
			}
		}
	}
	assert.Equal(t, len(series.Samples), inputPos, "expect to have returned all evaluations")
}

func TestRulerSharding(t *testing.T) {
	const numRulesGroups = 100

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Generate multiple rule groups, with 1 rule each.
	ruleGroups := make([]rulefmt.RuleGroup, numRulesGroups)
	expectedNames := make([]string, numRulesGroups)
	for i := 0; i < numRulesGroups; i++ {
		var recordNode yaml.Node
		var exprNode yaml.Node

		recordNode.SetString(fmt.Sprintf("rule_%d", i))
		exprNode.SetString(strconv.Itoa(i))
		ruleName := fmt.Sprintf("test_%d", i)

		expectedNames[i] = ruleName
		ruleGroups[i] = rulefmt.RuleGroup{
			Name:     ruleName,
			Interval: 60,
			Rules: []rulefmt.RuleNode{{
				Record: recordNode,
				Expr:   exprNode,
			}},
		}
	}

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, mimirBucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Configure the ruler.
	rulerFlags := mergeFlags(
		CommonStorageBackendFlags(),
		RulerFlags(),
		BlocksStorageFlags(),
		RulerShardingFlags(consul.NetworkHTTPEndpoint()),
		map[string]string{
			// Disable rule group limit
			"-ruler.max-rule-groups-per-tenant": "0",
		},
	)

	// Start rulers.
	ruler1 := e2emimir.NewRuler("ruler-1", consul.NetworkHTTPEndpoint(), rulerFlags)
	ruler2 := e2emimir.NewRuler("ruler-2", consul.NetworkHTTPEndpoint(), rulerFlags)
	rulers := e2emimir.NewCompositeMimirService(ruler1, ruler2)
	require.NoError(t, s.StartAndWaitReady(ruler1, ruler2))

	// Upload rule groups to one of the rulers.
	c, err := e2emimir.NewClient("", "", "", ruler1.HTTPEndpoint(), "user-1")
	require.NoError(t, err)

	for _, ruleGroup := range ruleGroups {
		require.NoError(t, c.SetRuleGroup(ruleGroup, "test"))
	}

	// Wait until rulers have loaded all rules.
	require.NoError(t, rulers.WaitSumMetricsWithOptions(e2e.Equals(numRulesGroups), []string{"cortex_prometheus_rule_group_rules"}, e2e.WaitMissingMetrics))

	// Since rulers have loaded all rules, we expect that rules have been sharded
	// between the two rulers.
	require.NoError(t, ruler1.WaitSumMetrics(e2e.Less(numRulesGroups), "cortex_prometheus_rule_group_rules"))
	require.NoError(t, ruler2.WaitSumMetrics(e2e.Less(numRulesGroups), "cortex_prometheus_rule_group_rules"))

	// Fetch the rules and ensure they match the configured ones.
	actualGroups, err := c.GetPrometheusRules()
	require.NoError(t, err)

	var actualNames []string
	for _, group := range actualGroups {
		actualNames = append(actualNames, group.Name)
	}
	assert.ElementsMatch(t, expectedNames, actualNames)
}

func TestRulerAlertmanager(t *testing.T) {
	var namespaceOne = "test_/encoded_+namespace/?"
	ruleGroup := createTestRuleGroup()

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, mimirBucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Have at least one alertmanager configuration.
	require.NoError(t, uploadAlertmanagerConfig(minio, mimirBucketName, "user-1", mimirAlertmanagerUserConfigYaml))

	// Start Alertmanagers.
	amFlags := mergeFlags(AlertmanagerFlags(), CommonStorageBackendFlags(), AlertmanagerShardingFlags(consul.NetworkHTTPEndpoint(), 1))
	am1 := e2emimir.NewAlertmanager("alertmanager1", amFlags)
	am2 := e2emimir.NewAlertmanager("alertmanager2", amFlags)
	require.NoError(t, s.StartAndWaitReady(am1, am2))

	am1URL := "http://" + am1.HTTPEndpoint()
	am2URL := "http://" + am2.HTTPEndpoint()

	// Configure the ruler.
	rulerFlags := mergeFlags(
		CommonStorageBackendFlags(),
		RulerFlags(),
		BlocksStorageFlags(),
		map[string]string{
			// Connect the ruler to Alertmanagers
			"-ruler.alertmanager-url": strings.Join([]string{am1URL, am2URL}, ","),
		},
	)

	// Start Ruler.
	ruler := e2emimir.NewRuler("ruler", consul.NetworkHTTPEndpoint(), rulerFlags)
	require.NoError(t, s.StartAndWaitReady(ruler))

	// Create a client with the ruler address configured
	c, err := e2emimir.NewClient("", "", "", ruler.HTTPEndpoint(), "user-1")
	require.NoError(t, err)

	// Set the rule group into the ruler
	require.NoError(t, c.SetRuleGroup(ruleGroup, namespaceOne))

	// Wait until the user manager is created
	require.NoError(t, ruler.WaitSumMetrics(e2e.Equals(1), "cortex_ruler_managers_total"))

	//  Wait until we've discovered the alertmanagers.
	require.NoError(t, ruler.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"cortex_prometheus_notifications_alertmanagers_discovered"}, e2e.WaitMissingMetrics))
}

func TestRulerAlertmanagerTLS(t *testing.T) {
	var namespaceOne = "test_/encoded_+namespace/?"
	ruleGroup := createTestRuleGroup()

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, mimirBucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// set the ca
	cert := ca.New("Ruler/Alertmanager Test")

	// Ensure the entire path of directories exist.
	require.NoError(t, os.MkdirAll(filepath.Join(s.SharedDir(), "certs"), os.ModePerm))

	require.NoError(t, cert.WriteCACertificate(filepath.Join(s.SharedDir(), caCertFile)))

	// server certificate
	require.NoError(t, cert.WriteCertificate(
		&x509.Certificate{
			Subject:     pkix.Name{CommonName: "client"},
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		},
		filepath.Join(s.SharedDir(), clientCertFile),
		filepath.Join(s.SharedDir(), clientKeyFile),
	))
	require.NoError(t, cert.WriteCertificate(
		&x509.Certificate{
			Subject:     pkix.Name{CommonName: "server"},
			DNSNames:    []string{"ruler.alertmanager-client"},
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		},
		filepath.Join(s.SharedDir(), serverCertFile),
		filepath.Join(s.SharedDir(), serverKeyFile),
	))

	// Have at least one alertmanager configuration.
	require.NoError(t, uploadAlertmanagerConfig(minio, mimirBucketName, "user-1", mimirAlertmanagerUserConfigYaml))

	// Start Alertmanagers.
	amFlags := mergeFlags(
		AlertmanagerFlags(),
		CommonStorageBackendFlags(),
		AlertmanagerShardingFlags(consul.NetworkHTTPEndpoint(), 1),
		getServerHTTPTLSFlags(),
	)
	am1 := e2emimir.NewAlertmanagerWithTLS("alertmanager1", amFlags)
	require.NoError(t, s.StartAndWaitReady(am1))

	// Configure the ruler.
	rulerFlags := mergeFlags(
		CommonStorageBackendFlags(),
		RulerFlags(),
		BlocksStorageFlags(),
		map[string]string{
			// Connect the ruler to the Alertmanager
			"-ruler.alertmanager-url": "https://" + am1.HTTPEndpoint(),
		},
	)

	// Start Ruler.
	ruler := e2emimir.NewRuler("ruler", consul.NetworkHTTPEndpoint(), rulerFlags)
	require.NoError(t, s.StartAndWaitReady(ruler))

	// Create a client with the ruler address configured
	c, err := e2emimir.NewClient("", "", "", ruler.HTTPEndpoint(), "user-1")
	require.NoError(t, err)

	// Set the rule group into the ruler
	require.NoError(t, c.SetRuleGroup(ruleGroup, namespaceOne))

	// Wait until the user manager is created
	require.NoError(t, ruler.WaitSumMetrics(e2e.Equals(1), "cortex_ruler_managers_total"))

	//  Wait until we've discovered the alertmanagers.
	require.NoError(t, ruler.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_prometheus_notifications_alertmanagers_discovered"}, e2e.WaitMissingMetrics))
}

func TestRulerMetricsForInvalidQueriesAndNoFetchedSeries(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, mimirBucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Configure the ruler.
	flags := mergeFlags(
		CommonStorageBackendFlags(),
		RulerFlags(),
		BlocksStorageFlags(),
		map[string]string{
			// Evaluate rules often, so that we don't need to wait for metrics to show up.
			"-ruler.evaluation-interval": "2s",
			"-ruler.poll-interval":       "2s",
			// No delay
			"-ruler.evaluation-delay-duration": "0",

			"-blocks-storage.tsdb.block-ranges-period":   "1h",
			"-blocks-storage.bucket-store.sync-interval": "1s",
			"-blocks-storage.tsdb.retention-period":      "2h",

			// We run single ingester only, no replication.
			"-ingester.ring.replication-factor": "1",

			// Very low limit so that ruler hits it.
			"-querier.max-fetched-chunks-per-query": "5",

			// Do not involve the block storage as we don't upload blocks.
			"-querier.query-store-after": "12h",

			// Enable query stats for ruler to test metric for no fetched series
			"-ruler.query-stats-enabled": "true",
		},
	)

	const namespace = "test"
	const user = "user"

	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	ruler := e2emimir.NewRuler("ruler", consul.NetworkHTTPEndpoint(), flags)
	ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(distributor, ingester, ruler))

	// Wait until both the distributor and ruler have updated the ring. The querier will also watch
	// the store-gateway ring if blocks sharding is enabled.
	// The distributor should have 512 tokens for the ingester ring and 1 for the distributor ring
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512+1), "cortex_ring_tokens_total"))
	// Ruler will see 512 tokens for the ingester, and 128 for the ruler.
	require.NoError(t, ruler.WaitSumMetrics(e2e.Equals(512+128), "cortex_ring_tokens_total"))

	c, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", ruler.HTTPEndpoint(), user)
	require.NoError(t, err)

	// Push some series to Mimir -- enough so that we can hit some limits.
	for i := 0; i < 10; i++ {
		series, _, _ := generateAlternatingSeries(i)("metric", time.Now(), prompb.Label{Name: "foo", Value: fmt.Sprintf("%d", i)})

		res, err := c.Push(series)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)
	}

	totalQueries, err := ruler.SumMetrics([]string{"cortex_ruler_queries_total"})
	require.NoError(t, err)

	addNewRuleAndWait := func(groupName, expression string, shouldFail bool) {
		require.NoError(t, c.SetRuleGroup(ruleGroupWithRecordingRule(groupName, "rule", expression), namespace))
		m := ruleGroupMatcher(user, namespace, groupName)

		// Wait until ruler has loaded the group.
		require.NoError(t, ruler.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_prometheus_rule_group_rules"}, e2e.WithLabelMatchers(m), e2e.WaitMissingMetrics))

		// Wait until rule group has tried to evaluate the rule, and succeeded.
		require.NoError(t, ruler.WaitSumMetricsWithOptions(e2e.GreaterOrEqual(1), []string{"cortex_prometheus_rule_evaluations_total"}, e2e.WithLabelMatchers(m), e2e.WaitMissingMetrics))

		if shouldFail {
			// Verify that evaluation of the rule failed.
			require.NoError(t, ruler.WaitSumMetricsWithOptions(e2e.GreaterOrEqual(1), []string{"cortex_prometheus_rule_evaluation_failures_total"}, e2e.WithLabelMatchers(m), e2e.WaitMissingMetrics))
		} else {
			// Verify that evaluation of the rule succeeded.
			require.NoError(t, ruler.WaitSumMetricsWithOptions(e2e.Equals(0), []string{"cortex_prometheus_rule_evaluation_failures_total"}, e2e.WithLabelMatchers(m), e2e.WaitMissingMetrics))
		}
	}

	deleteRuleAndWait := func(groupName string) {
		// Delete rule to prepare for next part of test.
		require.NoError(t, c.DeleteRuleGroup(namespace, groupName))

		// Wait until ruler has unloaded the group. We don't use any matcher, so there should be no groups (in fact, metric disappears).
		require.NoError(t, ruler.WaitSumMetricsWithOptions(e2e.Equals(0), []string{"cortex_prometheus_rule_group_rules"}, e2e.SkipMissingMetrics))
	}

	// Verify that user-failures don't increase cortex_ruler_queries_failed_total
	for groupName, expression := range map[string]string{
		// Syntactically correct expression (passes check in ruler), but failing because of invalid regex. This fails in PromQL engine.
		// This selects the label "nolabel" which does not exist, thus too many chunks doesn't apply.
		"invalid_group": `label_replace(metric{nolabel="none"}, "foo", "$1", "service", "[")`,

		// This one fails in querier code, because of limits.
		"too_many_chunks_group": `sum(metric)`,

		// Combine the errors above to have a compound error.
		"invalid_and_too_many_chunks_group": `label_replace(metric, "foo", "$1", "service", "[")`,
	} {
		t.Run(groupName, func(t *testing.T) {
			addNewRuleAndWait(groupName, expression, true)

			// Ensure that these failures were not reported as "failed queries"
			sum, err := ruler.SumMetrics([]string{"cortex_ruler_queries_failed_total"})
			require.NoError(t, err)
			require.Equal(t, float64(0), sum[0])

			// Delete rule before checking "cortex_ruler_queries_total", as we want to reuse value for next test.
			deleteRuleAndWait(groupName)

			// Check that cortex_ruler_queries_total went up since last test.
			newTotalQueries, err := ruler.SumMetrics([]string{"cortex_ruler_queries_total"})
			require.NoError(t, err)
			require.Greater(t, newTotalQueries[0], totalQueries[0])

			// Remember totalQueries for next test.
			totalQueries = newTotalQueries
		})
	}

	getZeroSeriesQueriesTotal := func() int {
		sum, err := ruler.SumMetrics([]string{"cortex_ruler_queries_zero_fetched_series_total"})
		require.NoError(t, err)
		return int(sum[0])
	}

	getLastEvalSamples := func() int {
		sum, err := ruler.SumMetrics([]string{"cortex_prometheus_last_evaluation_samples"})
		require.NoError(t, err)
		return int(sum[0])
	}

	// Now let's upload a non-failing rule, and make sure that it works.
	t.Run("real_error", func(t *testing.T) {
		const groupName = "good_rule"
		const expression = `sum(metric{foo=~"1|2"})`
		addNewRuleAndWait(groupName, expression, false)

		// Still no failures.
		sum, err := ruler.SumMetrics([]string{"cortex_ruler_queries_failed_total"})
		require.NoError(t, err)
		require.Equal(t, float64(0), sum[0])

		deleteRuleAndWait(groupName)
	})

	// Now let's test the metric for no fetched series.
	t.Run("no_fetched_series_metric", func(t *testing.T) {
		const groupName = "good_rule_with_fetched_series_but_no_samples"
		const expression = `sum(metric{foo=~"1|2"}) < -1`
		addNewRuleAndWait(groupName, expression, false)

		// Ensure that no samples were returned.
		require.Zero(t, getLastEvalSamples())

		// Ensure that the metric for no fetched series was not incremented.
		require.Zero(t, getZeroSeriesQueriesTotal())

		deleteRuleAndWait(groupName)
		zeroSeriesQueries := getZeroSeriesQueriesTotal()

		const groupName2 = "good_rule_with_fetched_series_and_samples"
		const expression2 = `sum(metric{foo=~"1|2"})`
		addNewRuleAndWait(groupName2, expression2, false)

		// Ensure that samples were returned.
		require.Less(t, 0, getLastEvalSamples())

		// Ensure that the metric for no fetched series was not incremented.
		require.Equal(t, zeroSeriesQueries, getZeroSeriesQueriesTotal())

		deleteRuleAndWait(groupName2)
		zeroSeriesQueries = getZeroSeriesQueriesTotal()

		const groupName3 = "good_rule_with_no_series_selector"
		const expression3 = `vector(1.2345)`
		addNewRuleAndWait(groupName3, expression3, false)

		// Ensure that samples were returned.
		require.Less(t, 0, getLastEvalSamples())

		// Ensure that the metric for no fetched series was not incremented.
		require.Equal(t, zeroSeriesQueries, getZeroSeriesQueriesTotal())

		deleteRuleAndWait(groupName3)
		zeroSeriesQueries = getZeroSeriesQueriesTotal()

		const groupName4 = "good_rule_with_fetched_series_and_samples_and_non_series_selector"
		const expression4 = `sum(metric{foo=~"1|2"}) + vector(1.2345)`
		addNewRuleAndWait(groupName4, expression4, false)

		// Ensure that samples were not returned.
		require.Less(t, 0, getLastEvalSamples())

		// Ensure that the metric for no fetched series was not incremented.
		require.Equal(t, zeroSeriesQueries, getZeroSeriesQueriesTotal())

		deleteRuleAndWait(groupName4)
		zeroSeriesQueries = getZeroSeriesQueriesTotal()

		const groupName5 = "good_rule_with_no_fetched_series_and_no_samples_and_non_series_selector"
		const expression5 = `sum(metric{foo="10000"}) + vector(1.2345)`
		addNewRuleAndWait(groupName5, expression5, false)

		// Ensure that no samples were returned.
		require.Zero(t, getLastEvalSamples())

		// Ensure that the metric for no fetched series was incremented.
		require.Less(t, zeroSeriesQueries, getZeroSeriesQueriesTotal())

		deleteRuleAndWait(groupName5)
		zeroSeriesQueries = getZeroSeriesQueriesTotal()

		const groupName6 = "good_rule_with_no_fetched_series_and_no_samples"
		const expression6 = `sum(metric{foo="10000"})`
		addNewRuleAndWait(groupName6, expression6, false)

		// Ensure that no samples were returned.
		require.Zero(t, getLastEvalSamples())

		// Ensure that the metric for no fetched series was incremented.
		require.Less(t, zeroSeriesQueries, getZeroSeriesQueriesTotal())
	})

	// Now let's stop ingester, and recheck metrics. This should increase cortex_ruler_queries_failed_total failures.
	t.Run("stop_ingester", func(t *testing.T) {
		require.NoError(t, s.Stop(ingester))

		// We should start getting "real" failures now.
		require.NoError(t, ruler.WaitSumMetricsWithOptions(e2e.GreaterOrEqual(1), []string{"cortex_ruler_queries_failed_total"}))
	})
}

func TestRulerFederatedRules(t *testing.T) {
	type testCase struct {
		name               string
		tenantsWithMetrics []string // will generate series `metric{}` in each tenant
		ruleGroupOwner     string   // will create the federated rule under this tenant
		ruleExpression     string
		groupSourceTenants []string

		assertEvalResult func(model.Vector)
	}

	testCases := []testCase{
		{
			name:               "separate source tenants and destination tenant",
			tenantsWithMetrics: []string{"tenant-1", "tenant-2"},
			ruleGroupOwner:     "tenant-3",
			ruleExpression:     "count(count_over_time(metric[1h]))",
			groupSourceTenants: []string{"tenant-1", "tenant-2"},
			assertEvalResult: func(evalResult model.Vector) {
				require.Len(t, evalResult, 1)
				require.Equal(t, evalResult[0].Value, model.SampleValue(2))
			},
		},
		{
			name:               "__tenant_id__ is added on all metrics for federated rules",
			tenantsWithMetrics: []string{"tenant-1", "tenant-2", "tenant-3"},
			ruleGroupOwner:     "tenant-3",
			ruleExpression:     "count(group by (__tenant_id__) (metric))", // count to number of different values of __tenant_id__
			groupSourceTenants: []string{"tenant-1", "tenant-2", "tenant-3"},
			assertEvalResult: func(evalResult model.Vector) {
				require.Len(t, evalResult, 1)
				require.Equal(t, evalResult[0].Value, model.SampleValue(3))
			},
		},
		{
			name:               "__tenant_id__ is present on metrics for federated rules when source tenants == owner",
			tenantsWithMetrics: []string{"tenant-1"},
			ruleGroupOwner:     "tenant-1",
			ruleExpression:     "count(group by (__tenant_id__) (metric))", // query to count to number of different values of __tenant_id__
			groupSourceTenants: []string{"tenant-1", "tenant-2", "tenant-3"},
			assertEvalResult: func(evalResult model.Vector) {
				require.Len(t, evalResult, 1)
				require.Equal(t, evalResult[0].Value, model.SampleValue(1))
			},
		},
	}

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	t.Cleanup(s.Close)

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, mimirBucketName)
	require.NoError(t, s.StartAndWaitReady(minio, consul))

	flags := mergeFlags(
		CommonStorageBackendFlags(),
		RulerFlags(),
		BlocksStorageFlags(),
		map[string]string{
			"-tenant-federation.enabled":        "true",
			"-ruler.tenant-federation.enabled":  "true",
			"-ingester.ring.replication-factor": "1",
		},
	)

	// Start up services
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags)
	ruler := e2emimir.NewRuler("ruler", consul.NetworkHTTPEndpoint(), flags)
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(distributor, ingester, ruler, querier))

	// Wait until both the distributor and ruler are ready
	// The distributor should have 512 tokens for the ingester ring and 1 for the distributor rin
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512+1), "cortex_ring_tokens_total"))
	// Ruler will see 512 tokens from ingester, and 128 tokens from itself.
	require.NoError(t, ruler.WaitSumMetrics(e2e.Equals(512+128), "cortex_ring_tokens_total"))

	// isolatedTestCase prefixes all the tenant IDs in the testCase with "run-<n>-"
	// so we can ensure that the tenants in different test cases don't overlap
	isolatedTestCase := func(tc testCase, n int) testCase {
		prefixID := func(tenantID string) string {
			return fmt.Sprintf("run-%d-%s", n, tenantID)
		}

		tc.ruleGroupOwner = prefixID(tc.ruleGroupOwner)
		for i, t := range tc.tenantsWithMetrics {
			tc.tenantsWithMetrics[i] = prefixID(t)
		}
		for i, t := range tc.groupSourceTenants {
			tc.groupSourceTenants[i] = prefixID(t)
		}
		return tc
	}

	for i, tc := range testCases {
		tc = isolatedTestCase(tc, i)
		t.Run(tc.name, func(t *testing.T) {
			// Generate some series under different tenants
			floatTs := time.Now()
			histTs := floatTs.Add(time.Second)
			for _, tenantID := range tc.tenantsWithMetrics {
				client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", tenantID)
				require.NoError(t, err)

				seriesFloat, _, _ := generateFloatSeries("metric", floatTs)
				seriesHist, _, _ := generateHistogramSeries("metric", histTs)

				res, err := client.Push(seriesFloat)
				require.NoError(t, err)
				require.Equal(t, 200, res.StatusCode)

				res, err = client.Push(seriesHist)
				require.NoError(t, err)
				require.Equal(t, 200, res.StatusCode)
			}

			// Create a client as owner tenant to upload groups and then make assertions
			c, err := e2emimir.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", ruler.HTTPEndpoint(), tc.ruleGroupOwner)
			require.NoError(t, err)

			// Obtain total series before rule evaluation
			totalSeriesBeforeEval, err := ingester.SumMetrics([]string{"cortex_ingester_memory_series"})
			require.NoError(t, err)

			// Create federated rule group
			namespace := "test_namespace"
			ruleName := "federated_rule_name"
			g := ruleGroupWithRecordingRule("x", ruleName, tc.ruleExpression)
			g.Interval = model.Duration(time.Second / 4)
			g.SourceTenants = tc.groupSourceTenants
			require.NoError(t, c.SetRuleGroup(g, namespace))

			// Wait until another user manager is created (i is one more since last time). This means the rule groups is loaded.
			require.NoError(t, ruler.WaitSumMetrics(e2e.Equals(float64(i+1)), "cortex_ruler_managers_total"))

			// Check to ensure the rules running in the ruler match what was set
			_, rgs, err := c.GetRuleGroups()
			retrievedNamespace, exists := rgs[namespace]
			require.NoError(t, err)
			require.True(t, exists)
			require.Len(t, retrievedNamespace, 1)
			require.ElementsMatch(t, retrievedNamespace[0].SourceTenants, tc.groupSourceTenants)

			// Wait until rule evaluation resulting series had been pushed
			require.NoError(t, ingester.WaitSumMetrics(e2e.Greater(totalSeriesBeforeEval[0]), "cortex_ingester_memory_series"))

			result, err := c.Query(ruleName, time.Now())
			require.NoError(t, err)
			tc.assertEvalResult(result.(model.Vector))
		})
	}
}

func TestRulerRemoteEvaluation(t *testing.T) {
	tcs := map[string]struct {
		tenantsWithMetrics       []string
		groupSourceTenants       []string
		ruleGroupOwner           string
		ruleExpression           string
		queryResultPayloadFormat string
		assertEvalResult         func(model.Vector)
	}{
		"non federated rule group": {
			tenantsWithMetrics: []string{"tenant-1"},
			ruleGroupOwner:     "tenant-1",
			ruleExpression:     "count(count_over_time(metric[1h]))",
			assertEvalResult: func(evalResult model.Vector) {
				require.Len(t, evalResult, 1)
				require.Equal(t, evalResult[0].Value, model.SampleValue(1))
			},
		},
		"federated rule group": {
			tenantsWithMetrics: []string{"tenant-2", "tenant-3"},
			ruleGroupOwner:     "tenant-3",
			ruleExpression:     "count(group by (__tenant_id__) (metric))",
			groupSourceTenants: []string{"tenant-2", "tenant-3"},
			assertEvalResult: func(evalResult model.Vector) {
				require.Len(t, evalResult, 1)
				require.Equal(t, evalResult[0].Value, model.SampleValue(2))
			},
		},
		"protobuf query result payload format": {
			tenantsWithMetrics:       []string{"tenant-4"},
			ruleGroupOwner:           "tenant-4",
			ruleExpression:           "count(count_over_time(metric[1h]))",
			queryResultPayloadFormat: "protobuf",
			assertEvalResult: func(evalResult model.Vector) {
				require.Len(t, evalResult, 1)
				require.Equal(t, evalResult[0].Value, model.SampleValue(1))
			},
		},
	}

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	t.Cleanup(s.Close)

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, mimirBucketName)
	require.NoError(t, s.StartAndWaitReady(minio, consul))

	flags := mergeFlags(
		CommonStorageBackendFlags(),
		RulerFlags(),
		BlocksStorageFlags(),
		map[string]string{
			"-tenant-federation.enabled":        "true",
			"-ruler.tenant-federation.enabled":  "true",
			"-ingester.ring.replication-factor": "1",
		},
	)

	// Start the query-frontend.
	queryFrontend := e2emimir.NewQueryFrontend("query-frontend", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.Start(queryFrontend))
	flags["-querier.frontend-address"] = queryFrontend.NetworkGRPCEndpoint()

	// Use query-frontend for rule evaluation.
	flags["-ruler.query-frontend.address"] = fmt.Sprintf("dns:///%s", queryFrontend.NetworkGRPCEndpoint())

	// Start up services
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags)
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)

	require.NoError(t, s.StartAndWaitReady(distributor, ingester, querier))
	require.NoError(t, s.WaitReady(queryFrontend))

	// Wait until the distributor is ready
	// The distributor should have 512 tokens for the ingester ring and 1 for the distributor ring
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512+1), "cortex_ring_tokens_total"))

	for tName, tc := range tcs {
		t.Run(tName, func(t *testing.T) {
			if tc.queryResultPayloadFormat == "" {
				delete(flags, "-ruler.query-frontend.query-result-response-format")
			} else {
				flags["-ruler.query-frontend.query-result-response-format"] = tc.queryResultPayloadFormat
			}

			ruler := e2emimir.NewRuler("ruler", consul.NetworkHTTPEndpoint(), flags)
			require.NoError(t, s.StartAndWaitReady(ruler))
			t.Cleanup(func() {
				_ = s.Stop(ruler)
			})

			// Ruler will see 512 tokens from ingester, and 128 tokens from itself.
			require.NoError(t, ruler.WaitSumMetrics(e2e.Equals(512+128), "cortex_ring_tokens_total"))

			// Generate some series under different tenants
			floatTs := time.Now()
			histTs := floatTs.Add(time.Second)
			for _, tenantID := range tc.tenantsWithMetrics {
				client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", tenantID)
				require.NoError(t, err)

				seriesFloat, _, _ := generateFloatSeries("metric", floatTs)
				seriesHist, _, _ := generateHistogramSeries("metric", histTs)

				res, err := client.Push(seriesFloat)
				require.NoError(t, err)
				require.Equal(t, 200, res.StatusCode)

				res, err = client.Push(seriesHist)
				require.NoError(t, err)
				require.Equal(t, 200, res.StatusCode)
			}

			// Create a client as owner tenant to upload groups and then make assertions
			c, err := e2emimir.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", ruler.HTTPEndpoint(), tc.ruleGroupOwner)
			require.NoError(t, err)

			// Obtain total series before rule evaluation
			totalSeriesBeforeEval, err := ingester.SumMetrics([]string{"cortex_ingester_memory_series"})
			require.NoError(t, err)

			// Obtain total rule managers before rule group creation
			managersTotalBeforeCreate, err := ruler.SumMetrics([]string{"cortex_ruler_managers_total"})
			require.NoError(t, err)

			// Create rule group
			namespace := "test_namespace"
			ruleName := "rule_name"
			g := ruleGroupWithRecordingRule("x", ruleName, tc.ruleExpression)
			g.Interval = model.Duration(time.Second / 4)
			g.SourceTenants = tc.groupSourceTenants
			require.NoError(t, c.SetRuleGroup(g, namespace))

			// Wait until another user manager is created.
			require.NoError(t, ruler.WaitSumMetrics(e2e.Greater(float64(managersTotalBeforeCreate[0])), "cortex_ruler_managers_total"))

			// Wait until rule evaluation resulting series had been pushed
			require.NoError(t, ingester.WaitSumMetrics(e2e.Greater(totalSeriesBeforeEval[0]), "cortex_ingester_memory_series"))

			// Ensure that expression evaluation was performed on the query-frontend service
			var queryEvalUser string
			if len(tc.groupSourceTenants) > 0 {
				queryEvalUser = tenant.JoinTenantIDs(tc.groupSourceTenants)
			} else {
				queryEvalUser = tc.ruleGroupOwner
			}
			require.NoError(t, queryFrontend.WaitSumMetricsWithOptions(
				e2e.GreaterOrEqual(1),
				[]string{"cortex_query_frontend_queries_total"},
				e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "user", queryEvalUser)),
			))

			// Assert rule evaluation result
			result, err := c.Query(ruleName, time.Now())
			require.NoError(t, err)
			tc.assertEvalResult(result.(model.Vector))
		})
	}
}

func TestRulerRemoteEvaluation_ShouldEnforceStrongReadConsistencyForDependentRulesWhenUsingTheIngestStorage(t *testing.T) {
	const (
		ruleGroupNamespace = "test"
		ruleGroupName      = "test"
	)

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	t.Cleanup(s.Close)

	flags := mergeFlags(
		CommonStorageBackendFlags(),
		RulerFlags(),
		BlocksStorageFlags(),
		IngestStorageFlags(),
		map[string]string{
			"-ingester.ring.replication-factor": "1",

			// No strong read consistency by default for this test. We want the ruler to enforce the strong
			// consistency when required.
			"-ingest-storage.read-consistency": api.ReadConsistencyEventual,
		},
	)

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, mimirBucketName)
	kafka := e2edb.NewKafka()
	require.NoError(t, s.StartAndWaitReady(minio, consul, kafka))

	// Start the query-frontend.
	queryFrontend := e2emimir.NewQueryFrontend("query-frontend", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.Start(queryFrontend))
	flags["-querier.frontend-address"] = queryFrontend.NetworkGRPCEndpoint()

	// Use query-frontend for rule evaluation.
	flags["-ruler.query-frontend.address"] = fmt.Sprintf("dns:///%s", queryFrontend.NetworkGRPCEndpoint())

	// Start up services
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	ingester := e2emimir.NewIngester("ingester-0", consul.NetworkHTTPEndpoint(), flags)
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)

	require.NoError(t, s.StartAndWaitReady(distributor, ingester, querier))
	require.NoError(t, s.WaitReady(queryFrontend))

	// Wait until the distributor is ready.
	// The distributor should have 512 tokens for the ingester ring and 1 for the distributor ring.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512+1), "cortex_ring_tokens_total"))

	// Wait until partitions are ACTIVE in the ring.
	for _, service := range []*e2emimir.MimirService{distributor, queryFrontend, querier} {
		require.NoError(t, service.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_partition_ring_partitions"}, e2e.WithLabelMatchers(
			labels.MustNewMatcher(labels.MatchEqual, "name", "ingester-partitions"),
			labels.MustNewMatcher(labels.MatchEqual, "state", "Active"))))
	}

	waitQueryFrontendToSuccessfullyFetchLastProducedOffsets(t, queryFrontend)

	client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), queryFrontend.HTTPEndpoint(), "", "", userID)
	require.NoError(t, err)

	// Push a test series.
	now := time.Now()
	series, _, _ := generateFloatSeries("series_1", now)

	res, err := client.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	t.Run("evaluation of independent rules should not require strong consistency", func(t *testing.T) {
		ruler := e2emimir.NewRuler("ruler", consul.NetworkHTTPEndpoint(), flags)
		require.NoError(t, s.StartAndWaitReady(ruler))
		t.Cleanup(func() {
			require.NoError(t, s.Stop(ruler))
		})

		rulerClient, err := e2emimir.NewClient("", "", "", ruler.HTTPEndpoint(), userID)
		require.NoError(t, err)

		// Create a rule group containing 2 independent rules.
		group := ruleGroupWithRules(ruleGroupName, time.Second,
			recordingRule("series_1:count", "count(series_1)"),
			recordingRule("series_1:sum", "sum(series_1)"),
		)
		require.NoError(t, rulerClient.SetRuleGroup(group, ruleGroupNamespace))

		// Cleanup the ruler config when the test will end, so that it doesn't interfere with other test cases.
		t.Cleanup(func() {
			require.NoError(t, rulerClient.DeleteRuleNamespace(ruleGroupNamespace))
		})

		// Wait until the rules are evaluated.
		require.NoError(t, ruler.WaitSumMetricsWithOptions(e2e.GreaterOrEqual(float64(len(group.Rules))), []string{"cortex_prometheus_rule_evaluations_total"}, e2e.WaitMissingMetrics))

		// The rules have been evaluated at least once. We expect the rule queries
		// have run with eventual consistency because they are independent.
		require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Equals(0), "cortex_ingest_storage_strong_consistency_requests_total"))
		require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(0), "cortex_ingest_storage_strong_consistency_requests_total"))

		// Ensure cortex_distributor_replication_factor is not exported when ingest storage is enabled
		// because it's how we detect whether a Mimir cluster is running with ingest storage.
		assertServiceMetricsNotMatching(t, "cortex_distributor_replication_factor", queryFrontend, distributor, ingester, querier, ruler)
	})

	t.Run("evaluation of dependent rules should require strong consistency", func(t *testing.T) {
		ruler := e2emimir.NewRuler("ruler", consul.NetworkHTTPEndpoint(), flags)
		require.NoError(t, s.StartAndWaitReady(ruler))
		t.Cleanup(func() {
			require.NoError(t, s.Stop(ruler))
		})

		rulerClient, err := e2emimir.NewClient("", "", "", ruler.HTTPEndpoint(), userID)
		require.NoError(t, err)

		// Create a rule group containing 2 rules: the 2nd one depends on the 1st one.
		group := ruleGroupWithRules(ruleGroupName, time.Second,
			recordingRule("series_1:count", "count(series_1)"),
			recordingRule("series_1:count:sum", "sum(series_1:count)"),
		)
		require.NoError(t, rulerClient.SetRuleGroup(group, ruleGroupNamespace))

		// Cleanup the ruler config when the test will end, so that it doesn't interfere with other test cases.
		t.Cleanup(func() {
			require.NoError(t, rulerClient.DeleteRuleNamespace(ruleGroupNamespace))
		})

		// Wait until the rules are evaluated.
		require.NoError(t, ruler.WaitSumMetricsWithOptions(e2e.GreaterOrEqual(float64(len(group.Rules))), []string{"cortex_prometheus_rule_evaluations_total"}, e2e.WaitMissingMetrics))

		// The rules have been evaluated at least once. We expect the 2nd rule query
		// has run with strong consistency because it depends on the 1st one.
		require.NoError(t, queryFrontend.WaitSumMetrics(e2e.GreaterOrEqual(1), "cortex_ingest_storage_strong_consistency_requests_total"))

		// We expect the offsets to be fetched by query-frontend and then propagated to ingesters.
		require.NoError(t, ingester.WaitSumMetricsWithOptions(e2e.GreaterOrEqual(1), []string{"cortex_ingest_storage_strong_consistency_requests_total"}, e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "with_offset", "true"))))
		require.NoError(t, ingester.WaitSumMetricsWithOptions(e2e.Equals(0), []string{"cortex_ingest_storage_strong_consistency_requests_total"}, e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "with_offset", "false"))))

		// Ensure cortex_distributor_replication_factor is not exported when ingest storage is enabled
		// because it's how we detect whether a Mimir cluster is running with ingest storage.
		assertServiceMetricsNotMatching(t, "cortex_distributor_replication_factor", queryFrontend, distributor, ingester, querier, ruler)
	})
}

func TestRuler_RestoreWithLongForPeriod(t *testing.T) {
	const (
		forGracePeriod    = 5 * time.Second
		groupEvalInterval = time.Second
		groupForPeriod    = 10 * groupEvalInterval

		// This is internal prometheus logic. It waits for two evaluations in order to have
		// enough data to evaluate the alert. Prometheus doesn't expose state which says
		// that the alert is restored, we wait for the third iteration after the restoration
		// as a witness that restoration was attempted.
		evalsToRestoredAlertState = 3
	)
	var (
		evalsForAlertToFire = math.Ceil(float64(groupForPeriod) / float64(groupEvalInterval))
	)
	require.Greater(t, evalsForAlertToFire, float64(evalsToRestoredAlertState), "in order to have a meaningful test, the alert should fire in more evaluations than is necessary to restore its state")
	require.Greater(t, groupForPeriod, forGracePeriod, "the \"for\" duration should be longer than the for grace period. The prometheus ruler only tries to restore the alert from storage if its \"for\" period is longer than the for_grace_period config parameter.")

	s, err := e2e.NewScenario(networkName)
	assert.NoError(t, err)
	t.Cleanup(s.Close)
	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, mimirBucketName)
	assert.NoError(t, s.StartAndWaitReady(minio, consul))

	flags := mergeFlags(
		CommonStorageBackendFlags(),
		RulerFlags(),
		BlocksStorageFlags(),
		map[string]string{
			"-ruler.for-grace-period":           forGracePeriod.String(),
			"-auth.multitenancy-enabled":        "true",
			"-ingester.ring.replication-factor": "1",
			"-log.level":                        "debug",
		},
	)

	// Start up services
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags)
	ruler := e2emimir.NewRuler("ruler", consul.NetworkHTTPEndpoint(), flags)
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
	assert.NoError(t, s.StartAndWaitReady(distributor, ingester, ruler, querier))

	// Wait until both the distributor and ruler are ready
	// The distributor should have 512 tokens for the ingester ring and 1 for the distributor ring
	assert.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512+1), "cortex_ring_tokens_total"))
	// Ruler will see 512 tokens from ingester, and 128 tokens from itself.
	assert.NoError(t, ruler.WaitSumMetrics(e2e.Equals(512+128), "cortex_ring_tokens_total"))

	// Create a client to upload and query rule groups
	c, err := e2emimir.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", ruler.HTTPEndpoint(), "tenant-1")
	assert.NoError(t, err)

	// Create an alert rule which always fires
	g := ruleGroupWithAlertingRule("group_name", "rule_name", "1")
	g.Interval = model.Duration(groupEvalInterval)
	g.Rules[0].For = model.Duration(groupForPeriod)
	assert.NoError(t, c.SetRuleGroup(g, "test_namespace"))

	// Wait until the alert has had time to start firing
	assert.NoError(t, ruler.WaitSumMetricsWithOptions(e2e.Greater(evalsForAlertToFire), []string{"cortex_prometheus_rule_evaluations_total"}, e2e.WaitMissingMetrics))

	// Assert that the alert is firing
	rules, err := c.GetPrometheusRules()
	assert.NoError(t, err)
	assert.Equal(t, "firing", rules[0].Rules[0].(v1.AlertingRule).State)

	// Restart ruler to trigger an alert state restoration
	assert.NoError(t, s.Stop(ruler))
	assert.NoError(t, s.StartAndWaitReady(ruler))
	assert.NoError(t, ruler.WaitSumMetrics(e2e.Equals(512+128), "cortex_ring_tokens_total"))

	// Recreate client because ports may have changed
	c, err = e2emimir.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", ruler.HTTPEndpoint(), "tenant-1")
	assert.NoError(t, err)

	// Wait for actual restoration to happen
	assert.NoError(t, ruler.WaitSumMetricsWithOptions(e2e.GreaterOrEqual(evalsToRestoredAlertState), []string{"cortex_prometheus_rule_evaluations_total"}, e2e.WaitMissingMetrics))

	// Assert the alert is already firing
	rules, err = c.GetPrometheusRules()
	assert.NoError(t, err)
	assert.Equal(t, "firing", rules[0].Rules[0].(v1.AlertingRule).State)
}

func TestRulerProtectedNamespaces(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, mimirBucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	const (
		protectedNamespaceOne = "namespace-protected-1"
		protectedNamespaceTwo = "namespace-protected-2"
	)

	// Configure the ruler.
	rulerFlags := mergeFlags(CommonStorageBackendFlags(), RulerFlags(), BlocksStorageFlags(), map[string]string{
		"-ruler.protected-namespaces": strings.Join([]string{protectedNamespaceOne, protectedNamespaceTwo}, ","),
	})

	// Start Mimir components.
	ruler := e2emimir.NewRuler("ruler", consul.NetworkHTTPEndpoint(), rulerFlags)
	require.NoError(t, s.StartAndWaitReady(ruler))

	// Create two clients, one with the override header and one without for the same user - we'll need them both.
	client, err := e2emimir.NewClient("", "", "", ruler.HTTPEndpoint(), "user-1")
	require.NoError(t, err)
	cWithOverrideHeader, err := e2emimir.NewClient("", "", "", ruler.HTTPEndpoint(), "user-1", e2emimir.WithAddHeader(
		mimir_ruler.OverrideProtectionHeader, protectedNamespaceOne))
	require.NoError(t, err)
	cWithOverrideHeader2, err := e2emimir.NewClient("", "", "", ruler.HTTPEndpoint(), "user-1", e2emimir.WithAddHeader(
		mimir_ruler.OverrideProtectionHeader, protectedNamespaceTwo))
	require.NoError(t, err)

	const nonProtectedNamespace = "namespace1"

	t.Run("without protection overrides", func(t *testing.T) {
		// Create a rule group in one of the protected namespaces so that the headers we get back are set correctly.
		setupRg := createTestRuleGroup(withName("protected-rg"))
		require.NoError(t, cWithOverrideHeader.SetRuleGroup(setupRg, protectedNamespaceOne))

		t.Run("on a non-protected namespace", func(t *testing.T) {
			rgnp1 := createTestRuleGroup(withName("rgnp1"))
			// Create two rule groups successfully.
			require.NoError(t, client.SetRuleGroup(rgnp1, nonProtectedNamespace))
			require.NoError(t, client.SetRuleGroup(createTestRuleGroup(withName("rgnp2")), nonProtectedNamespace))
			// List all rule groups successfully.
			resp, rgs, err := client.GetRuleGroups()
			require.Len(t, rgs, 2)
			require.Len(t, rgs[nonProtectedNamespace], 2)
			require.Len(t, rgs[protectedNamespaceOne], 1)
			require.Equal(t, resp.Header.Get(mimir_ruler.ProtectedNamespacesHeader), protectedNamespaceOne)
			require.NoError(t, err)
			// Get a rule group successfully.
			resp, err = client.GetRuleGroup(nonProtectedNamespace, rgnp1.Name)
			require.Equal(t, resp.Header.Get(mimir_ruler.ProtectedNamespacesHeader), "") // No namespace header unless requesting the protected namespace.
			require.NoError(t, err)
			// Delete a rule group successfully.
			require.NoError(t, client.DeleteRuleGroup(nonProtectedNamespace, rgnp1.Name))
			// Delete a namespace successfully.
			require.NoError(t, client.DeleteRuleNamespace(nonProtectedNamespace))
		})

		t.Run("on a protected namespace", func(t *testing.T) {
			// Create a rule group in the protected namespace fails.
			require.EqualError(t, client.SetRuleGroup(createTestRuleGroup(), protectedNamespaceOne), "unexpected status code: 403")
			// List all rule groups successfully.
			resp, rgs, err := client.GetRuleGroups()
			require.Len(t, rgs, 1)
			require.Len(t, rgs[protectedNamespaceOne], 1)
			require.Equal(t, resp.Header.Get(mimir_ruler.ProtectedNamespacesHeader), protectedNamespaceOne)
			require.NoError(t, err)
			// Get the rule group we originally created successfully.
			resp, err = client.GetRuleGroup(protectedNamespaceOne, setupRg.Name)
			require.Equal(t, resp.Header.Get(mimir_ruler.ProtectedNamespacesHeader), protectedNamespaceOne)
			require.NoError(t, err)
			// Deleting the rule group we created as part of the setup fails.
			require.EqualError(t, client.DeleteRuleGroup(protectedNamespaceOne, setupRg.Name), "unexpected status code: 403")
			// Deleting a namespace we create as part of the setup fails.
			require.EqualError(t, client.DeleteRuleNamespace(protectedNamespaceOne), "unexpected status code: 403")
		})
	})

	t.Run("with protection overrides", func(t *testing.T) {
		t.Run("on a non-protected namespace", func(t *testing.T) {
			rgnp1 := createTestRuleGroup(withName("rgnp1"))
			// Create two rule groups successfully.
			require.NoError(t, cWithOverrideHeader.SetRuleGroup(rgnp1, nonProtectedNamespace))
			require.NoError(t, cWithOverrideHeader.SetRuleGroup(createTestRuleGroup(withName("rgnp2")), nonProtectedNamespace))
			// List all rule groups successfully.
			resp, rgs, err := cWithOverrideHeader.GetRuleGroups()
			require.Len(t, rgs, 2)
			require.Len(t, rgs[nonProtectedNamespace], 2)
			require.Len(t, rgs[protectedNamespaceOne], 1)
			require.Equal(t, resp.Header.Get(mimir_ruler.ProtectedNamespacesHeader), protectedNamespaceOne)
			require.NoError(t, err)
			// Get a rule group successfully.
			resp, err = cWithOverrideHeader.GetRuleGroup(nonProtectedNamespace, rgnp1.Name)
			require.Equal(t, resp.Header.Get(mimir_ruler.ProtectedNamespacesHeader), "") // No namespace header unless requesting the protected namespace.
			require.NoError(t, err)
			// Delete a rule group successfully.
			require.NoError(t, cWithOverrideHeader.DeleteRuleGroup(nonProtectedNamespace, rgnp1.Name))
			// Delete a namespace successfully.
			require.NoError(t, cWithOverrideHeader.DeleteRuleNamespace(nonProtectedNamespace))
		})

		t.Run("on a protected namespace", func(t *testing.T) {
			rgp1 := createTestRuleGroup(withName("rgp1"))
			// Create another rule group successfully. We created another one as part of the setup.
			require.NoError(t, cWithOverrideHeader.SetRuleGroup(rgp1, protectedNamespaceOne))
			// List all rule groups successfully.
			resp, rgs, err := cWithOverrideHeader.GetRuleGroups()
			require.Len(t, rgs, 1)
			require.Len(t, rgs[protectedNamespaceOne], 2)
			require.Equal(t, resp.Header.Get(mimir_ruler.ProtectedNamespacesHeader), protectedNamespaceOne)
			require.NoError(t, err)
			// Get a rule group successfully.
			resp, err = cWithOverrideHeader.GetRuleGroup(protectedNamespaceOne, rgp1.Name)
			require.Equal(t, resp.Header.Get(mimir_ruler.ProtectedNamespacesHeader), protectedNamespaceOne) // No namespace header unless requesting the protected namespace.
			require.NoError(t, err)
			// Delete a rule group successfully.
			require.NoError(t, cWithOverrideHeader.DeleteRuleGroup(protectedNamespaceOne, rgp1.Name))
			// Delete a namespace successfully.
			require.NoError(t, cWithOverrideHeader.DeleteRuleNamespace(protectedNamespaceOne))
		})
	})

	t.Run("with multiple namespaces protected", func(t *testing.T) {
		// Create a rule group in one of the protected namespaces so that the headers we get back are set correctly.
		setupRg := createTestRuleGroup(withName("protected-rg"))
		require.NoError(t, cWithOverrideHeader.SetRuleGroup(setupRg, protectedNamespaceOne))

		// You can't modify the protected namespace without the correct override header.
		// We're using the client that has the override header set for protectedNamespaceOne.
		rgp2 := createTestRuleGroup(withName("protected-rg2"))
		require.EqualError(t, cWithOverrideHeader.SetRuleGroup(rgp2, protectedNamespaceTwo), "unexpected status code: 403")

		// With the right client, it succeeds.
		require.NoError(t, cWithOverrideHeader2.SetRuleGroup(rgp2, protectedNamespaceTwo))

		// When listing rules, the header should give you all protected namespaces.
		resp, rgs, err := cWithOverrideHeader.GetRuleGroups()

		require.Len(t, rgs, 2)
		require.Len(t, rgs[protectedNamespaceOne], 1)
		require.Len(t, rgs[protectedNamespaceTwo], 1)
		require.NoError(t, err)
		require.ElementsMatch(t, []string{protectedNamespaceOne, protectedNamespaceTwo}, strings.Split(resp.Header.Get(mimir_ruler.ProtectedNamespacesHeader), ","))
	})
}

func TestRulerPerRuleConcurrency(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, mimirBucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Configure the ruler.
	rulerFlags := mergeFlags(CommonStorageBackendFlags(), RulerFlags(), BlocksStorageFlags(), map[string]string{
		// Evaluate rules often.
		"-ruler.evaluation-interval": "5s",
		// No delay
		"-ruler.evaluation-delay-duration":                                       "0",
		"-ruler.poll-interval":                                                   "2s",
		"-ruler.max-independent-rule-evaluation-concurrency":                     "4",
		"-ruler.max-independent-rule-evaluation-concurrency-per-tenant":          "2",
		"-ruler.independent-rule-evaluation-concurrency-min-duration-percentage": "0", // This makes sure no matter the ratio, we will attempt concurrency.
	})

	// Start Mimir components.
	ruler := e2emimir.NewRuler("ruler", consul.NetworkHTTPEndpoint(), rulerFlags)
	require.NoError(t, s.StartAndWaitReady(ruler))

	// Upload rule groups to one of the rulers for two users.
	c, err := e2emimir.NewClient("", "", "", ruler.HTTPEndpoint(), "user-1")
	require.NoError(t, err)
	c2, err := e2emimir.NewClient("", "", "", ruler.HTTPEndpoint(), "user-2")
	require.NoError(t, err)

	rg := rulefmt.RuleGroup{
		Name:     "nameX",
		Interval: 5,
		Rules: []rulefmt.RuleNode{
			recordingRule("tenant_vector_one", "count(series_1)"),
			recordingRule("tenant_vector_two", "count(series_1)"),
			recordingRule("tenant_vector_three", "count(series_1)"),
			recordingRule("tenant_vector_four", "count(series_1)"),
			recordingRule("tenant_vector_five", "count(series_1)"),
			recordingRule("tenant_vector_six", "count(series_1)"),
			recordingRule("tenant_vector_seven", "count(series_1)"),
			recordingRule("tenant_vector_eight", "count(series_1)"),
			recordingRule("tenant_vector_nine", "count(series_1)"),
			recordingRule("tenant_vector_ten", "count(series_1)"),
		},
	}

	require.NoError(t, c.SetRuleGroup(rg, "fileY"))
	require.NoError(t, c2.SetRuleGroup(rg, "fileY"))

	// Wait until rulers have loaded all rules.
	require.NoError(t, ruler.WaitSumMetricsWithOptions(e2e.Equals(20), []string{"cortex_prometheus_rule_group_rules"}, e2e.WaitMissingMetrics))

	// We should have 20 attempts and 20 or less for failed or successful attempts to acquire the lock.
	require.NoError(t, ruler.WaitSumMetricsWithOptions(e2e.Equals(20), []string{"cortex_ruler_independent_rule_evaluation_concurrency_attempts_started_total"}, e2e.WaitMissingMetrics))
	// The magic number here is because we have a maximum per tenant concurrency of 2. So we expect at least 4 (2 slots * 2 tenants) to complete successfully.
	require.NoError(t, ruler.WaitSumMetricsWithOptions(e2e.GreaterOrEqual(4), []string{"cortex_ruler_independent_rule_evaluation_concurrency_attempts_completed_total"}, e2e.WaitMissingMetrics))
	require.NoError(t, ruler.WaitSumMetricsWithOptions(func(sums ...float64) bool {
		return e2e.SumValues(sums) == 20
	}, []string{"cortex_ruler_independent_rule_evaluation_concurrency_attempts_completed_total", "cortex_ruler_independent_rule_evaluation_concurrency_attempts_incomplete_total"}, e2e.WaitMissingMetrics))
}

func TestRulerEnableAPIs(t *testing.T) {
	testCases := []struct {
		name                        string
		apiEnabled                  bool
		expectedRegisteredEndpoints [][2]string
		expectedMissingEndpoints    [][2]string
	}{
		{
			name:       "API is disabled",
			apiEnabled: false,

			expectedRegisteredEndpoints: [][2]string{
				{http.MethodGet, "/prometheus/api/v1/alerts"},
				{http.MethodGet, "/prometheus/api/v1/rules"},
			},
			expectedMissingEndpoints: [][2]string{
				{http.MethodGet, "/api/v1/rules"},
				{http.MethodGet, "/api/v1/rules/my_namespace"},
				{http.MethodGet, "/api/v1/rules/my_namespace/my_group"},
				{http.MethodPost, "/api/v1/rules/my_namespace"},

				{http.MethodGet, "/prometheus/rules"},
				{http.MethodGet, "/prometheus/rules/my_namespace"},
				{http.MethodGet, "/prometheus/rules/my_namespace/my_group"},
				{http.MethodPost, "/prometheus/rules/my_namespace"},

				{http.MethodGet, "/prometheus/config/v1/rules"},
				{http.MethodGet, "/prometheus/config/v1/rules/my_namespace"},
				{http.MethodGet, "/prometheus/config/v1/rules/my_namespace/my_group"},
				{http.MethodPost, "/prometheus/config/v1/rules/my_namespace"},
			},
		},
		{
			name:       "API is enabled",
			apiEnabled: true,
			expectedRegisteredEndpoints: [][2]string{
				// not going to test GET /api/v1/rules/my_namespace/my_group because it requires creating a rule group
				{http.MethodGet, "/prometheus/api/v1/alerts"},
				{http.MethodGet, "/prometheus/api/v1/rules"},

				{http.MethodGet, "/prometheus/config/v1/rules"},
				{http.MethodGet, "/prometheus/config/v1/rules/my_namespace"},
				{http.MethodPost, "/prometheus/config/v1/rules/my_namespace"},
			},

			expectedMissingEndpoints: [][2]string{
				{http.MethodGet, "/api/v1/rules"},
				{http.MethodGet, "/api/v1/rules/my_namespace"},
				{http.MethodPost, "/api/v1/rules/my_namespace"},

				{http.MethodGet, "/prometheus/rules"},
				{http.MethodGet, "/prometheus/rules/my_namespace"},
				{http.MethodPost, "/prometheus/rules/my_namespace"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := e2e.NewScenario(networkName)
			require.NoError(t, err)
			defer s.Close()

			// Start dependencies.
			consul := e2edb.NewConsul()
			minio := e2edb.NewMinio(9000, mimirBucketName)
			require.NoError(t, s.StartAndWaitReady(consul, minio))

			// Configure the ruler.
			rulerFlags := mergeFlags(CommonStorageBackendFlags(), RulerFlags(), BlocksStorageFlags(), map[string]string{
				"-ruler.enable-api": fmt.Sprintf("%t", tc.apiEnabled),
			})

			// Start Mimir components.
			ruler := e2emimir.NewRuler("ruler", consul.NetworkHTTPEndpoint(), rulerFlags)
			require.NoError(t, s.StartAndWaitReady(ruler))

			runTest := func(name, method, path string, shouldBeFound bool) {
				t.Run(name, func(t *testing.T) {
					client, err := e2emimir.NewClient("", "", "", "", "fake")
					require.NoError(t, err)

					url := "http://" + ruler.HTTPEndpoint() + path

					var resp *http.Response
					switch method {
					case http.MethodGet:
						resp, err = client.DoGet(url)
					case http.MethodPost:
						resp, err = client.DoPost(url, nil)
					default:
						require.Contains(t, []string{http.MethodGet, http.MethodPost}, method, "test only supports POST and GET")
					}

					assert.NoError(t, err)
					if shouldBeFound {
						assert.NotEqual(t, resp.StatusCode, http.StatusNotFound)
					} else {
						assert.Equal(t, resp.StatusCode, http.StatusNotFound)
					}
				})
			}

			for _, ep := range tc.expectedRegisteredEndpoints {
				method, path := ep[0], ep[1]
				name := "!=404_" + method + "_" + path
				runTest(name, method, path, true)
			}

			for _, ep := range tc.expectedMissingEndpoints {
				method, path := ep[0], ep[1]
				name := "==404_" + method + "_" + path
				runTest(name, method, path, false)
			}
		})
	}
}

func ruleGroupMatcher(user, namespace, groupName string) *labels.Matcher {
	return labels.MustNewMatcher(labels.MatchEqual, "rule_group", fmt.Sprintf("data-ruler/%s/%s;%s", user, namespace, groupName))
}

func ruleGroupWithRecordingRule(groupName string, ruleName string, expression string) rulefmt.RuleGroup {
	return ruleGroupWithRules(groupName, 10, recordingRule(ruleName, expression))
}

func ruleGroupWithAlertingRule(groupName string, ruleName string, expression string) rulefmt.RuleGroup {
	return ruleGroupWithRules(groupName, 10, alertingRule(ruleName, expression))
}

func ruleGroupWithRules(groupName string, interval time.Duration, rules ...rulefmt.RuleNode) rulefmt.RuleGroup {
	return rulefmt.RuleGroup{
		Name:     groupName,
		Interval: model.Duration(interval),
		Rules:    rules,
	}
}

func withName(name string) testRuleGroupsOption {
	return func(rg *rulefmt.RuleGroup) {
		rg.Name = name
	}
}

type testRuleGroupsOption func(*rulefmt.RuleGroup)

func createTestRuleGroup(opts ...testRuleGroupsOption) rulefmt.RuleGroup {
	rg := ruleGroupWithRules("test_encoded_+\"+group_name/?", 100, recordingRule("test_rule", "up"))

	for _, opt := range opts {
		opt(&rg)
	}

	return rg
}

func recordingRule(record, expr string) rulefmt.RuleNode {
	var recordNode = yaml.Node{}
	var exprNode = yaml.Node{}

	recordNode.SetString(record)
	exprNode.SetString(expr)

	return rulefmt.RuleNode{
		Record: recordNode,
		Expr:   exprNode,
	}
}

func alertingRule(alert, expr string) rulefmt.RuleNode {
	var alertNode = yaml.Node{}
	var exprNode = yaml.Node{}

	alertNode.SetString(alert)
	exprNode.SetString(expr)

	return rulefmt.RuleNode{
		Alert: alertNode,
		Expr:  exprNode,
		For:   30,
	}
}
