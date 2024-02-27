// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/ingester_limits_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
//go:build requires_docker

package integration

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
)

func TestIngesterGlobalLimits(t *testing.T) {
	tests := map[string]struct {
		tenantShardSize          int
		maxGlobalSeriesPerTenant int
		maxGlobalSeriesPerMetric int
	}{
		"shuffle sharding disabled": {
			tenantShardSize:          0,
			maxGlobalSeriesPerTenant: 1000,
			maxGlobalSeriesPerMetric: 300,
		},
		"shuffle sharding enabled": {
			tenantShardSize:          1,
			maxGlobalSeriesPerTenant: 1000,
			maxGlobalSeriesPerMetric: 300,
		},
	}

	ossTests := map[string]bool{
		"ownedSeriesService enabled":  true,
		"ownedSeriesService disabled": false,
	}

	for testName, testData := range tests {
		for ossName, ossEnabled := range ossTests {
			t.Run(fmt.Sprintf("%s/%s", testName, ossName), func(t *testing.T) {
				s, err := e2e.NewScenario(networkName)
				require.NoError(t, err)
				defer s.Close()

				flags := mergeFlags(
					BlocksStorageFlags(),
					BlocksStorageS3Flags(),
				)
				flags["-ingester.ring.replication-factor"] = "1"
				flags["-distributor.ingestion-tenant-shard-size"] = strconv.Itoa(testData.tenantShardSize)
				flags["-ingester.max-global-series-per-user"] = strconv.Itoa(testData.maxGlobalSeriesPerTenant)
				flags["-ingester.max-global-series-per-metric"] = strconv.Itoa(testData.maxGlobalSeriesPerMetric)
				flags["-ingester.ring.heartbeat-period"] = "1s"
				flags["-ingester.track-ingester-owned-series"] = strconv.FormatBool(ossEnabled)
				flags["-ingester.use-ingester-owned-series-for-limits"] = strconv.FormatBool(ossEnabled)

				// Start dependencies.
				consul := e2edb.NewConsul()
				minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
				require.NoError(t, s.StartAndWaitReady(consul, minio))

				// Start Mimir components.
				distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
				ingester1 := e2emimir.NewIngester("ingester-1", consul.NetworkHTTPEndpoint(), flags)
				ingester2 := e2emimir.NewIngester("ingester-2", consul.NetworkHTTPEndpoint(), flags)
				ingester3 := e2emimir.NewIngester("ingester-3", consul.NetworkHTTPEndpoint(), flags)
				require.NoError(t, s.StartAndWaitReady(distributor, ingester1, ingester2, ingester3))

				// Wait until distributor has updated the ring.
				require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
					labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
					labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

				// Wait until ingesters have heartbeated the ring after all ingesters were active,
				// in order to update the number of instances. Since we have no metric, we have to
				// rely on a ugly sleep.
				time.Sleep(2 * time.Second)

				now := time.Now()
				client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", userID)
				require.NoError(t, err)

				numSeriesWithSameMetricName := 0
				numSeriesTotal := 0
				maxErrorsBeforeStop := 100

				// Try to push as many series with the same metric name as we can.
				for i, errs := 0, 0; i < 10000; i++ {
					series, _, _ := generateAlternatingSeries(i)("test_limit_per_metric", now, prompb.Label{
						Name:  "cardinality",
						Value: strconv.Itoa(rand.Int()),
					})

					res, err := client.Push(series)
					require.NoError(t, err)

					if res.StatusCode == 200 {
						numSeriesTotal++
						numSeriesWithSameMetricName++
					} else {
						require.Equal(t, 400, res.StatusCode)
						if errs++; errs >= maxErrorsBeforeStop {
							break
						}
					}
				}

				// Try to push as many series with the different metric name as we can.
				for i, errs := 0, 0; i < 10000; i++ {
					series, _, _ := generateAlternatingSeries(i)(fmt.Sprintf("test_limit_per_tenant_%d", rand.Int()), now)
					res, err := client.Push(series)
					require.NoError(t, err)

					if res.StatusCode == 200 {
						numSeriesTotal++
					} else {
						require.Equal(t, 400, res.StatusCode)
						if errs++; errs >= maxErrorsBeforeStop {
							break
						}
					}
				}

				// We expect the number of series we've been successfully pushed to be around
				// the limit. Due to how the global limit implementation works (lack of centralised
				// coordination) the actual number of written series could be slightly different
				// than the global limit, so we allow a 10% difference.
				delta := 0.1
				assert.InDelta(t, testData.maxGlobalSeriesPerMetric, numSeriesWithSameMetricName, float64(testData.maxGlobalSeriesPerMetric)*delta)
				assert.InDelta(t, testData.maxGlobalSeriesPerTenant, numSeriesTotal, float64(testData.maxGlobalSeriesPerTenant)*delta)

				// Ensure no service-specific metrics prefix is used by the wrong service.
				assertServiceMetricsPrefixes(t, Distributor, distributor)
				assertServiceMetricsPrefixes(t, Ingester, ingester1)
				assertServiceMetricsPrefixes(t, Ingester, ingester2)
				assertServiceMetricsPrefixes(t, Ingester, ingester3)
			})
		}
	}
}

func TestIngesterDynamicLimits(t *testing.T) {
	const (
		overridesFile     = "overrides.yaml"
		overridesTemplate = `
overrides:
  anonymous:
    max_global_series_per_user:    {{.MaxGlobalSeriesPerTenant}}
    max_global_series_per_metric:  {{.MaxGlobalSeriesPerMetric}}
    max_global_exemplars_per_user: {{.MaxGlobalExemplarsPerUser}}
`
	)
	tests := map[string]struct {
		MaxGlobalSeriesPerTenant  int
		MaxGlobalSeriesPerMetric  int
		MaxGlobalExemplarsPerUser int
	}{
		"lower limits": {
			MaxGlobalSeriesPerTenant: 100,
			MaxGlobalSeriesPerMetric: 30,
		},
		"higher limits": {
			MaxGlobalSeriesPerTenant:  1000,
			MaxGlobalSeriesPerMetric:  300,
			MaxGlobalExemplarsPerUser: 100,
		},
	}
	generators := map[string]generateNSeriesFunc{
		"float series":     generateNFloatSeries,
		"histogram series": generateNHistogramSeries,
	}

	ossTests := map[string]bool{
		"ownedSeriesService enabled":  true,
		"ownedSeriesService disabled": false,
	}

	for testName, testData := range tests {
		for fName, generator := range generators {
			for ossName, ossEnabled := range ossTests {
				t.Run(fmt.Sprintf("%s/%s/%s", testName, fName, ossName), func(t *testing.T) {
					s, err := e2e.NewScenario(networkName)
					require.NoError(t, err)
					defer s.Close()

					// Write blank overrides file, so we can check if they are updated later
					require.NoError(t, writeFileToSharedDir(s, overridesFile, []byte{}))

					// Start Cortex in single binary mode, reading the config from file.
					require.NoError(t, copyFileToSharedDir(s, "docs/configurations/single-process-config-blocks.yaml", mimirConfigFile))

					flags := map[string]string{
						"-runtime-config.reload-period":                  "100ms",
						"-blocks-storage.backend":                        "filesystem",
						"-blocks-storage.filesystem.dir":                 "/tmp",
						"-blocks-storage.storage-prefix":                 "blocks",
						"-ruler-storage.backend":                         "filesystem",
						"-ruler-storage.local.directory":                 "/tmp", // Avoid warning "unable to list rules".
						"-runtime-config.file":                           filepath.Join(e2e.ContainerSharedDir, overridesFile),
						"-ingester.track-ingester-owned-series":          strconv.FormatBool(ossEnabled),
						"-ingester.use-ingester-owned-series-for-limits": strconv.FormatBool(ossEnabled),
					}
					cortex1 := e2emimir.NewSingleBinary("cortex-1", flags, e2emimir.WithConfigFile(mimirConfigFile), e2emimir.WithPorts(9009, 9095))
					require.NoError(t, s.StartAndWaitReady(cortex1))

					// Populate the overrides we want, then wait long enough for it to be read.
					// (max-exemplars will be taken from config on first sample received)
					overrides := buildConfigFromTemplate(overridesTemplate, &testData)
					require.NoError(t, writeFileToSharedDir(s, overridesFile, []byte(overrides)))
					time.Sleep(500 * time.Millisecond)

					now := time.Now()
					client, err := e2emimir.NewClient(cortex1.HTTPEndpoint(), "", "", "", userID)
					require.NoError(t, err)

					numSeriesWithSameMetricName := 0
					numSeriesTotal := 0
					maxErrorsBeforeStop := 1

					// Try to push as many series with the same metric name as we can.
					for i, errs := 0, 0; i < 10000; i++ {
						series, _ := generator(10, 1, func() string { return "test_limit_per_metric" }, now, func() []prompb.Label {
							return []prompb.Label{{
								Name:  "cardinality",
								Value: strconv.Itoa(rand.Int()),
							}}
						})

						res, err := client.Push(series)
						require.NoError(t, err)

						if res.StatusCode == 200 {
							numSeriesTotal += 10
							numSeriesWithSameMetricName += 10
						} else {
							require.Equal(t, 400, res.StatusCode)
							if errs++; errs >= maxErrorsBeforeStop {
								break
							}
						}
					}

					// Try to push as many series with the different metric name as we can.
					for i, errs := 0, 0; i < 10000; i++ {
						series, _ := generator(10, 1, func() string { return fmt.Sprintf("test_limit_per_tenant_%d", rand.Int()) }, now, nil)
						res, err := client.Push(series)
						require.NoError(t, err)

						if res.StatusCode == 200 {
							numSeriesTotal += 10
						} else {
							require.Equal(t, 400, res.StatusCode)
							if errs++; errs >= maxErrorsBeforeStop {
								break
							}
						}
					}

					// With just one ingester we expect to hit the limit exactly
					assert.Equal(t, testData.MaxGlobalSeriesPerMetric, numSeriesWithSameMetricName)
					assert.Equal(t, testData.MaxGlobalSeriesPerTenant, numSeriesTotal)

					// Check metrics
					metricNumSeries, err := cortex1.SumMetrics([]string{"cortex_ingester_memory_series"})
					require.NoError(t, err)
					assert.Equal(t, testData.MaxGlobalSeriesPerTenant, int(e2e.SumValues(metricNumSeries)))
					metricNumExemplars, err := cortex1.SumMetrics([]string{"cortex_ingester_tsdb_exemplar_exemplars_in_storage"})
					require.NoError(t, err)
					assert.Equal(t, testData.MaxGlobalExemplarsPerUser, int(e2e.SumValues(metricNumExemplars)))
				})
			}
		}
	}
}

type RampingMetricsGenerator struct {
	wg     sync.WaitGroup
	client *e2emimir.Client
}

func (g *RampingMetricsGenerator) Start(startSeries, maxSeries, rampSeries, periodSec int) {
	g.wg.Add(1)
	go g.run(startSeries, maxSeries, rampSeries, periodSec)
}

func (g *RampingMetricsGenerator) Wait() {
	g.wg.Wait()
}

func (g *RampingMetricsGenerator) run(startSeries, maxSeries, rampSeries, periodMillis int) {
	defer g.wg.Done()

	ticker := time.NewTicker(time.Duration(periodMillis) * time.Millisecond)
	defer ticker.Stop()

	iteration := 0
	series := make([]prompb.TimeSeries, 0, maxSeries)
	for {
		<-ticker.C

		now := time.Now()
		numSeries := startSeries + (iteration * rampSeries)
		series = series[:0]
		for i := 0; i < numSeries; i++ {
			s, _, _ := generateFloatSeries(fmt.Sprintf("test_series_%d", i), now)
			series = append(series, s[0])
		}
		_, _ = g.client.Push(series)
		iteration++

		if numSeries >= maxSeries {
			return
		}
	}
}

func TestIngesterOwnedSeriesLimitsAddShards(t *testing.T) {
	const (
		overridesFile     = "overrides.yaml"
		overridesTemplate = `
overrides:
  e2e-user:
    ingestion_tenant_shard_size: {{.IngestionTenantShardSize}}
`
		maxGlobalSeriesPerUser = 10000
		initialShardCount      = 1
		finalShardCount        = 2
	)

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	flags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
	)
	flags["-ingester.ring.replication-factor"] = "1"
	flags["-ingester.ring.zone-awareness-enabled"] = "true"
	flags["-ingester.ring.instance-availability-zone"] = "zone-a"
	flags["-distributor.ingestion-tenant-shard-size"] = strconv.Itoa(initialShardCount)
	flags["-distributor.ingestion-rate-limit"] = "1000000"
	flags["-distributor.ingestion-burst-size"] = "10000000"
	flags["-ingester.max-global-series-per-user"] = strconv.Itoa(maxGlobalSeriesPerUser)
	flags["-ingester.ring.heartbeat-period"] = "100ms"
	flags["-runtime-config.reload-period"] = "100ms"
	flags["-runtime-config.file"] = filepath.Join(e2e.ContainerSharedDir, overridesFile)
	flags["-ingester.track-ingester-owned-series"] = "true"
	flags["-ingester.use-ingester-owned-series-for-limits"] = "true"
	flags["-ingester.owned-series-update-interval"] = "1s"

	// Write blank overrides file, so we can check if they are updated later
	require.NoError(t, writeFileToSharedDir(s, overridesFile, []byte{}))

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start Mimir components.
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	ingester1 := e2emimir.NewIngester("ingester-1", consul.NetworkHTTPEndpoint(), flags)
	ingester2 := e2emimir.NewIngester("ingester-2", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(distributor, ingester1, ingester2))

	// Wait until distributor has updated the ring.
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	// Wait until ingesters have heartbeated the ring after all ingesters were active,
	// in order to update the number of instances. Since we have no metric, we have to
	// rely on an ugly sleep.
	time.Sleep(200 * time.Millisecond)

	client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", userID)
	require.NoError(t, err)

	totalSeries := 9000
	gen := RampingMetricsGenerator{
		client: client,
	}
	gen.Start(4800, totalSeries, 100, 100)

	// Wait for ingesters to start receiving samples. We don't know which ingester
	// will get the series initially, so sleep.
	time.Sleep(200 * time.Millisecond)

	// Verify we're writing samples, but only to one ingester
	values, err := ingester1.SumMetrics([]string{"cortex_ingester_memory_series"})
	require.NoError(t, err)
	memSeries1 := e2e.SumValues(values)
	values, err = ingester2.SumMetrics([]string{"cortex_ingester_memory_series"})
	require.NoError(t, err)
	memSeries2 := e2e.SumValues(values)
	require.Greater(t, int(memSeries1+memSeries2), 0)
	require.Equalf(t, memSeries1+memSeries2, max(memSeries1, memSeries2), "ingester 1 series: %d, ingester 2 series: %d", int(memSeries1), int(memSeries2))

	// Populate the overrides we want
	overrides := buildConfigFromTemplate(overridesTemplate, struct{ IngestionTenantShardSize int }{IngestionTenantShardSize: finalShardCount})
	require.NoError(t, writeFileToSharedDir(s, overridesFile, []byte(overrides)))

	// Wait for metrics generator to finish
	gen.Wait()

	// Wait for metrics to get updated -- metrics are updated only every 15 seconds :/
	time.Sleep(20 * time.Second)

	// Check that local limits were correctly updated
	values, err = ingester1.SumMetrics([]string{"cortex_ingester_local_limits"})
	require.NoError(t, err)
	assert.Equal(t, maxGlobalSeriesPerUser/finalShardCount, int(e2e.SumValues(values)))
	values, err = ingester1.SumMetrics([]string{"cortex_ingester_local_limits"})
	require.NoError(t, err)
	assert.Equal(t, maxGlobalSeriesPerUser/finalShardCount, int(e2e.SumValues(values)))

	// Check in-memory series counts
	values, err = ingester1.SumMetrics([]string{"cortex_ingester_memory_series"})
	require.NoError(t, err)
	memSeries1 = e2e.SumValues(values)
	values, err = ingester2.SumMetrics([]string{"cortex_ingester_memory_series"})
	require.NoError(t, err)
	memSeries2 = e2e.SumValues(values)
	require.Greater(t, int(memSeries1+memSeries2), totalSeries, "ingester 1 series: %d, ingester 2 series: %d", int(memSeries1), int(memSeries2))

	// Check owned series counts
	values, err = ingester1.SumMetrics([]string{"cortex_ingester_owned_series"})
	require.NoError(t, err)
	ownedSeries1 := e2e.SumValues(values)
	values, err = ingester2.SumMetrics([]string{"cortex_ingester_owned_series"})
	require.NoError(t, err)
	ownedSeries2 := e2e.SumValues(values)
	require.Equalf(t, totalSeries, int(ownedSeries1+ownedSeries2), "ingester 1 series: %d, ingester 2 series: %d", int(ownedSeries1), int(ownedSeries2))

	// Check discards (metrics should not exist)
	values, err = ingester1.SumMetrics([]string{"cortex_discarded_samples_total"})
	assert.Error(t, err, "ingester 1 discards: %d", int(e2e.SumValues(values)))
	values, err = ingester2.SumMetrics([]string{"cortex_discarded_samples_total"})
	assert.Error(t, err, "ingester 2 discards: %d", int(e2e.SumValues(values)))
}

func TestIngesterOwnedSeriesLimitsAddIngester(t *testing.T) {
	const (
		maxGlobalSeriesPerUser = 10000
	)

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	flags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
	)
	flags["-ingester.ring.replication-factor"] = "1"
	flags["-ingester.ring.zone-awareness-enabled"] = "true"
	flags["-ingester.ring.instance-availability-zone"] = "zone-a"
	flags["-distributor.ingestion-tenant-shard-size"] = "100"
	flags["-distributor.ingestion-rate-limit"] = "1000000"
	flags["-distributor.ingestion-burst-size"] = "10000000"
	flags["-ingester.max-global-series-per-user"] = strconv.Itoa(maxGlobalSeriesPerUser)
	flags["-ingester.ring.heartbeat-period"] = "100ms"
	flags["-ingester.track-ingester-owned-series"] = "true"
	flags["-ingester.use-ingester-owned-series-for-limits"] = "true"
	flags["-ingester.owned-series-update-interval"] = "1s"

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start Mimir components (only 1 ingester for now).
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	ingester1 := e2emimir.NewIngester("ingester-1", consul.NetworkHTTPEndpoint(), flags)
	ingester2 := e2emimir.NewIngester("ingester-2", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(distributor, ingester1))

	// Wait until distributor has updated the ring.
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	// Wait until ingesters have heartbeated the ring after all ingesters were active,
	// in order to update the number of instances. Since we have no metric, we have to
	// rely on a ugly sleep.
	time.Sleep(200 * time.Millisecond)

	client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", userID)
	require.NoError(t, err)

	totalSeries := 9000
	gen := RampingMetricsGenerator{
		client: client,
	}
	gen.Start(4800, totalSeries, 100, 100)

	// Wait until ingester starts receiving metrics
	require.NoError(t, ingester1.WaitSumMetrics(e2e.Greater(0), "cortex_ingester_memory_series"))

	// Start second ingester
	require.NoError(t, s.StartAndWaitReady(ingester2))

	// Wait for metrics generator to finish
	gen.Wait()

	// Wait for owned series service to run and metrics to get updated
	// Metrics are updated only every 15 seconds :/
	time.Sleep(20 * time.Second)

	// Check that local limits were correctly updated
	values, err := ingester1.SumMetrics([]string{"cortex_ingester_local_limits"})
	require.NoError(t, err)
	assert.Equal(t, maxGlobalSeriesPerUser/2, int(e2e.SumValues(values)))
	values, err = ingester1.SumMetrics([]string{"cortex_ingester_local_limits"})
	require.NoError(t, err)
	assert.Equal(t, maxGlobalSeriesPerUser/2, int(e2e.SumValues(values)))

	// Check in-memory series counts
	values, err = ingester1.SumMetrics([]string{"cortex_ingester_memory_series"})
	require.NoError(t, err)
	memSeries1 := e2e.SumValues(values)
	values, err = ingester2.SumMetrics([]string{"cortex_ingester_memory_series"})
	require.NoError(t, err)
	memSeries2 := e2e.SumValues(values)
	require.Greater(t, int(memSeries1+memSeries2), totalSeries, "ingester 1 series: %d, ingester 2 series: %d", int(memSeries1), int(memSeries2))

	// Check owned series counts
	values, err = ingester1.SumMetrics([]string{"cortex_ingester_owned_series"})
	require.NoError(t, err)
	ownedSeries1 := e2e.SumValues(values)
	values, err = ingester2.SumMetrics([]string{"cortex_ingester_owned_series"})
	require.NoError(t, err)
	ownedSeries2 := e2e.SumValues(values)
	require.Equalf(t, totalSeries, int(ownedSeries1+ownedSeries2), "ingester 1 series: %d, ingester 2 series: %d", int(ownedSeries1), int(ownedSeries2))

	// Check discards (metrics should not exist)
	values, err = ingester1.SumMetrics([]string{"cortex_discarded_samples_total"})
	assert.Error(t, err, "ingester 1 discards: %d", int(e2e.SumValues(values)))
	values, err = ingester2.SumMetrics([]string{"cortex_discarded_samples_total"})
	assert.Error(t, err, "ingester 2 discards: %d", int(e2e.SumValues(values)))
}
