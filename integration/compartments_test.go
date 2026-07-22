// SPDX-License-Identifier: AGPL-3.0-only

package integration

import (
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/grafana/dskit/test"
	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
	"github.com/grafana/mimir/pkg/compartments"
)

// TestIngesterQuerying_ShouldSupportCompartments runs the compartments architecture end to end with
// two read and two write compartments, and verifies queries served from ingesters work across it.
func TestIngesterQuerying_ShouldSupportCompartments(t *testing.T) {
	const (
		numWriteCompartments = 2
		numReadCompartments  = 2
	)

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	baseFlags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
		IngestStorageFlags(e2edb.KafkaAuthNone),
		CompartmentsFlags(numWriteCompartments, numReadCompartments),
	)

	// Start dependencies: Consul, MinIO, and one Kafka cluster per write compartment.
	consul := e2edb.NewConsul()

	// Each read compartment owns a dedicated blocks-storage bucket; create one per read compartment.
	blocksBucketNameTemplate, compartmentBuckets := compartmentBlocksBucketNames(baseFlags["-blocks-storage.s3.bucket-name"], numReadCompartments)
	minio := e2edb.NewMinio(9000, compartmentBuckets...)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	kafkas := make([]e2e.Service, numWriteCompartments)
	for wc := range kafkas {
		kafkas[wc] = e2edb.NewKafka(e2edb.WithKafkaName(fmt.Sprintf("kafka-wc-%d", wc)))
	}
	require.NoError(t, s.StartAndWaitReady(kafkas...))

	// Start the query-scheduler and wire the query-frontend and querier to it.
	queryScheduler := e2emimir.NewQueryScheduler("query-scheduler", baseFlags)
	require.NoError(t, s.StartAndWaitReady(queryScheduler))
	baseFlags = mergeFlags(baseFlags, map[string]string{
		"-query-frontend.scheduler-address": queryScheduler.NetworkGRPCEndpoint(),
		"-querier.scheduler-address":        queryScheduler.NetworkGRPCEndpoint(),
	})

	// One distributor per write compartment, each producing to its own Kafka cluster.
	distributors := make([]*e2emimir.MimirService, numWriteCompartments)
	for wc := range distributors {
		distributors[wc] = e2emimir.NewDistributor(fmt.Sprintf("distributor-wc-%d", wc), consul.NetworkHTTPEndpoint(), mergeFlags(baseFlags, map[string]string{
			"-distributor.write-compartment-id": strconv.Itoa(wc),
		}))
	}

	// One ingester (partition 0) per read compartment. The trailing "-0" sets the partition ID to 0,
	// and -ingester.read-compartment-id selects the compartment ring it registers into.
	ingesters := make([]*e2emimir.MimirService, numReadCompartments)
	for rc := range ingesters {
		ingesters[rc] = e2emimir.NewIngester(fmt.Sprintf("ingester-rc-%d-0", rc), consul.NetworkHTTPEndpoint(), mergeFlags(baseFlags, map[string]string{
			"-ingester.read-compartment-id":  strconv.Itoa(rc),
			"-blocks-storage.s3.bucket-name": compartmentBuckets[rc],
		}))
	}

	queryFrontend := e2emimir.NewQueryFrontend("query-frontend", consul.NetworkHTTPEndpoint(), baseFlags)
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), mergeFlags(baseFlags, map[string]string{
		"-blocks-storage.s3.bucket-name": blocksBucketNameTemplate,
	}))

	mimirServices := []e2e.Service{queryFrontend, querier}
	for _, d := range distributors {
		mimirServices = append(mimirServices, d)
	}
	for _, i := range ingesters {
		mimirServices = append(mimirServices, i)
	}
	require.NoError(t, s.StartAndWaitReady(mimirServices...))

	// Wait until the query-frontend has updated the querier ring.
	require.NoError(t, queryFrontend.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "querier"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	// The ingester instance ring is shared across compartments, so the distributor and querier should
	// see every ingester ACTIVE in it.
	for _, svc := range []*e2emimir.MimirService{distributors[0], querier} {
		require.NoErrorf(t, svc.WaitSumMetricsWithOptions(e2e.Equals(numReadCompartments), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
			labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
			labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))),
			"service: %s", svc.Name())
	}

	// Each read compartment has its own partition ring; wait until its partition is ACTIVE, observed
	// from the components that route to it.
	for rc := 0; rc < numReadCompartments; rc++ {
		ringName := fmt.Sprintf("ingester-partitions-rc-%d", rc)
		for _, svc := range []*e2emimir.MimirService{distributors[0], querier, queryFrontend} {
			require.NoErrorf(t, svc.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_partition_ring_partitions"}, e2e.WithLabelMatchers(
				labels.MustNewMatcher(labels.MatchEqual, "name", ringName),
				labels.MustNewMatcher(labels.MatchEqual, "state", "Active"))),
				"service: %s ring: %s", svc.Name(), ringName)
		}
	}

	// Wait until the query-frontend has fetched the last produced offsets from every write compartment's
	// Kafka cluster at least once. This avoids querying before the topics exist in every cluster.
	waitQueryFrontendToFetchOffsetsPerWriteCompartment(t, queryFrontend, numWriteCompartments)

	// Pick, per read compartment, one distinct metric name to produce through each write compartment.
	// All names of a compartment route to that compartment regardless of which distributor produces them.
	router := compartments.NewRouter(numReadCompartments)
	namesByCompartment := metricNamesPerCompartment(router, numReadCompartments, numWriteCompartments)

	pushClients := make([]*e2emimir.Client, numWriteCompartments)
	for wc := range pushClients {
		client, err := e2emimir.NewClient(distributors[wc].HTTPEndpoint(), "", "", "", userID)
		require.NoError(t, err)
		pushClients[wc] = client
	}

	now := time.Now()
	expectedVectors := map[string]model.Vector{}

	for rc := 0; rc < numReadCompartments; rc++ {
		for wc := 0; wc < numWriteCompartments; wc++ {
			name := namesByCompartment[rc][wc]
			series, expectedVector, _ := generateFloatSeries(name, now)

			// Retry the push: right after a topic is auto-created, its partition leader may briefly be
			// unavailable, which fails the produce with a 500.
			client := pushClients[wc]
			test.Poll(t, 10*time.Second, true, func() interface{} {
				res, err := client.Push(series)
				return err == nil && res.StatusCode == 200
			})

			expectedVectors[name] = expectedVector
		}
	}

	// Query every series back through the query-frontend with strong read consistency.
	queryClient, err := e2emimir.NewClient("", queryFrontend.HTTPEndpoint(), "", "", userID)
	require.NoError(t, err)

	for name, expectedVector := range expectedVectors {
		result, err := queryClient.Query(name, now)
		require.NoErrorf(t, err, "metric: %s", name)
		require.Equalf(t, model.ValVector, result.Type(), "metric: %s", name)
		assert.Equalf(t, expectedVector, result.(model.Vector), "metric: %s", name)
	}

	// Sharding: each compartment's ingester holds only its own series (one produced through each write
	// compartment), confirming series sharded by metric name across read compartments.
	for rc := 0; rc < numReadCompartments; rc++ {
		require.NoErrorf(t, ingesters[rc].WaitSumMetrics(e2e.Equals(numWriteCompartments), "cortex_ingester_memory_series"),
			"read compartment: %d", rc)
	}
}

// TestStoreGatewayQuerying_ShouldSupportCompartments runs the compartments architecture end to end with
// two read and two write compartments, and verifies queries served from the store-gateway (blocks) work
// across it: series shipped to each read compartment's bucket are queried back through the compartment's
// own store-gateway, and a query is fanned out only to the read compartments it targets.
func TestStoreGatewayQuerying_ShouldSupportCompartments(t *testing.T) {
	const (
		numWriteCompartments = 2
		numReadCompartments  = 2
	)

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	baseFlags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
		IngestStorageFlags(e2edb.KafkaAuthNone),
		CompartmentsFlags(numWriteCompartments, numReadCompartments),
		map[string]string{
			// Blocks are created only by the explicit flush below, so keep the head block range well above
			// the test duration to avoid auto head-compaction splitting series across blocks (the idle
			// timeout already defaults to 1h).
			"-blocks-storage.tsdb.block-ranges-period": "1h",
			// Process the flush-triggered compaction and ship the resulting block quickly.
			"-blocks-storage.tsdb.head-compaction-interval": "1s",
			"-blocks-storage.tsdb.ship-interval":            "1s",
			// Sync shipped blocks into the store-gateway quickly.
			"-blocks-storage.bucket-store.sync-interval": "1s",
			// Force every query onto the store-gateway (never the ingester), so a result provably comes from blocks.
			"-querier.query-ingesters-within": "1ms",
			// Disable query sharding so each query is a single querier invocation, keeping the fan-out assertions crisp.
			"-query-frontend.query-sharding-total-shards": "0",
		},
	)

	// Start dependencies: Consul, MinIO, and one Kafka cluster per write compartment.
	consul := e2edb.NewConsul()

	// Each read compartment owns a dedicated blocks-storage bucket; create one per read compartment.
	blocksBucketNameTemplate, compartmentBuckets := compartmentBlocksBucketNames(baseFlags["-blocks-storage.s3.bucket-name"], numReadCompartments)
	minio := e2edb.NewMinio(9000, compartmentBuckets...)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	kafkas := make([]e2e.Service, numWriteCompartments)
	for wc := range kafkas {
		kafkas[wc] = e2edb.NewKafka(e2edb.WithKafkaName(fmt.Sprintf("kafka-wc-%d", wc)))
	}
	require.NoError(t, s.StartAndWaitReady(kafkas...))

	// Start the query-scheduler and wire the query-frontend and querier to it.
	queryScheduler := e2emimir.NewQueryScheduler("query-scheduler", baseFlags)
	require.NoError(t, s.StartAndWaitReady(queryScheduler))
	baseFlags = mergeFlags(baseFlags, map[string]string{
		"-query-frontend.scheduler-address": queryScheduler.NetworkGRPCEndpoint(),
		"-querier.scheduler-address":        queryScheduler.NetworkGRPCEndpoint(),
	})

	// One distributor per write compartment, each producing to its own Kafka cluster.
	distributors := make([]*e2emimir.MimirService, numWriteCompartments)
	for wc := range distributors {
		distributors[wc] = e2emimir.NewDistributor(fmt.Sprintf("distributor-wc-%d", wc), consul.NetworkHTTPEndpoint(), mergeFlags(baseFlags, map[string]string{
			"-distributor.write-compartment-id": strconv.Itoa(wc),
		}))
	}

	// One ingester (partition 0) per read compartment, shipping its blocks to its own bucket.
	ingesters := make([]*e2emimir.MimirService, numReadCompartments)
	for rc := range ingesters {
		ingesters[rc] = e2emimir.NewIngester(fmt.Sprintf("ingester-rc-%d-0", rc), consul.NetworkHTTPEndpoint(), mergeFlags(baseFlags, map[string]string{
			"-ingester.read-compartment-id":  strconv.Itoa(rc),
			"-blocks-storage.s3.bucket-name": compartmentBuckets[rc],
		}))
	}

	// One compactor per read compartment, writing its bucket's index (which the querier's blocks finder needs).
	compactors := make([]*e2emimir.MimirService, numReadCompartments)
	for rc := range compactors {
		compactors[rc] = e2emimir.NewCompactor(fmt.Sprintf("compactor-rc-%d", rc), consul.NetworkHTTPEndpoint(), mergeFlags(baseFlags, map[string]string{
			"-compactor.read-compartment-id": strconv.Itoa(rc),
			"-blocks-storage.s3.bucket-name": compartmentBuckets[rc],
		}))
	}

	// One store-gateway per read compartment, reading its own bucket and registering into its own ring.
	storeGateways := make([]*e2emimir.MimirService, numReadCompartments)
	for rc := range storeGateways {
		storeGateways[rc] = e2emimir.NewStoreGateway(fmt.Sprintf("store-gateway-rc-%d", rc), consul.NetworkHTTPEndpoint(), mergeFlags(baseFlags, map[string]string{
			"-store-gateway.read-compartment-id": strconv.Itoa(rc),
			"-blocks-storage.s3.bucket-name":     compartmentBuckets[rc],
		}))
	}

	queryFrontend := e2emimir.NewQueryFrontend("query-frontend", consul.NetworkHTTPEndpoint(), baseFlags)
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), mergeFlags(baseFlags, map[string]string{
		// The querier reads every compartment's bucket, so it gets the placeholder template and resolves each
		// compartment's bucket internally.
		"-blocks-storage.s3.bucket-name": blocksBucketNameTemplate,
	}))

	mimirServices := []e2e.Service{queryFrontend, querier}
	for _, d := range distributors {
		mimirServices = append(mimirServices, d)
	}
	for _, i := range ingesters {
		mimirServices = append(mimirServices, i)
	}
	for _, c := range compactors {
		mimirServices = append(mimirServices, c)
	}
	for _, sg := range storeGateways {
		mimirServices = append(mimirServices, sg)
	}
	require.NoError(t, s.StartAndWaitReady(mimirServices...))

	// Wait until the query-frontend has updated the querier ring.
	require.NoError(t, queryFrontend.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "querier"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	// The ingester instance ring is shared across compartments, so the distributor and querier should
	// see every ingester ACTIVE in it.
	for _, svc := range []*e2emimir.MimirService{distributors[0], querier} {
		require.NoErrorf(t, svc.WaitSumMetricsWithOptions(e2e.Equals(numReadCompartments), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
			labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
			labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))),
			"service: %s", svc.Name())
	}

	// Each read compartment has its own partition ring; wait until its partition is ACTIVE, observed from
	// the components that route to it. Every distributor produces to each read compartment's partition, so
	// all of them (not just one) must see it ACTIVE.
	partitionRingWatchers := append([]*e2emimir.MimirService{querier, queryFrontend}, distributors...)
	for rc := 0; rc < numReadCompartments; rc++ {
		ringName := fmt.Sprintf("ingester-partitions-rc-%d", rc)
		for _, svc := range partitionRingWatchers {
			require.NoErrorf(t, svc.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_partition_ring_partitions"}, e2e.WithLabelMatchers(
				labels.MustNewMatcher(labels.MatchEqual, "name", ringName),
				labels.MustNewMatcher(labels.MatchEqual, "state", "Active"))),
				"service: %s ring: %s", svc.Name(), ringName)
		}
	}

	// Each read compartment has its own store-gateway ring; wait until the store-gateway is ACTIVE in it,
	// observed both from the store-gateway itself and from the querier's per-compartment client ring.
	for rc := 0; rc < numReadCompartments; rc++ {
		require.NoErrorf(t, storeGateways[rc].WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
			labels.MustNewMatcher(labels.MatchEqual, "name", fmt.Sprintf("store-gateway-rc-%d", rc)),
			labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))),
			"read compartment: %d", rc)
		require.NoErrorf(t, querier.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
			labels.MustNewMatcher(labels.MatchEqual, "name", fmt.Sprintf("store-gateway-client-rc-%d", rc)),
			labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))),
			"read compartment: %d", rc)
	}

	// Wait until the query-frontend has fetched the last produced offsets from every write compartment's
	// Kafka cluster at least once. This avoids querying before the topics exist in every cluster.
	waitQueryFrontendToFetchOffsetsPerWriteCompartment(t, queryFrontend, numWriteCompartments)

	// Pick, per read compartment, one distinct metric name to produce through each write compartment.
	router := compartments.NewRouter(numReadCompartments)
	namesByCompartment := metricNamesPerCompartment(router, numReadCompartments, numWriteCompartments)

	pushClients := make([]*e2emimir.Client, numWriteCompartments)
	for wc := range pushClients {
		client, err := e2emimir.NewClient(distributors[wc].HTTPEndpoint(), "", "", "", userID)
		require.NoError(t, err)
		pushClients[wc] = client
	}

	now := time.Now()
	expectedVectors := map[string]model.Vector{}

	for rc := 0; rc < numReadCompartments; rc++ {
		for wc := 0; wc < numWriteCompartments; wc++ {
			name := namesByCompartment[rc][wc]
			series, expectedVector, _ := generateFloatSeries(name, now)

			// Retry the push: right after a topic is auto-created, its partition leader may briefly be
			// unavailable, which fails the produce with a 500.
			client := pushClients[wc]
			test.Poll(t, 10*time.Second, true, func() interface{} {
				res, err := client.Push(series)
				return err == nil && res.StatusCode == 200
			})

			expectedVectors[name] = expectedVector
		}
	}

	// Wait until each read compartment's ingester has consumed all its series (one produced through each
	// write compartment's Kafka cluster), then flush its head to a block and ship it. The created-series
	// counter is cumulative, so it's a stable gate even once the series are compacted out of the head.
	for rc := 0; rc < numReadCompartments; rc++ {
		require.NoErrorf(t, ingesters[rc].WaitSumMetrics(e2e.Equals(numWriteCompartments), "cortex_ingester_memory_series_created_total"),
			"read compartment: %d", rc)

		flushIngesterBlocks(t, ingesters[rc])

		require.NoErrorf(t, ingesters[rc].WaitSumMetrics(e2e.GreaterOrEqual(1), "cortex_ingester_shipper_uploads_total"),
			"read compartment: %d", rc)
	}

	// Wait until each read compartment's store-gateway has loaded its one block. Each compartment reads a
	// different bucket, so a store-gateway only ever loads its own compartment's block.
	for rc := 0; rc < numReadCompartments; rc++ {
		require.NoErrorf(t, storeGateways[rc].WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_bucket_store_blocks_loaded"}, e2e.WaitMissingMetrics),
			"read compartment: %d", rc)
	}

	queryClient, err := e2emimir.NewClient("", queryFrontend.HTTPEndpoint(), "", "", userID)
	require.NoError(t, err)
	// The store-gateway query path (bucket-index load, block sync, index-header build) is slower than the
	// 5s default, especially for the single-shot fan-out assertions below which aren't retried.
	queryClient.SetTimeout(30 * time.Second)

	// Query every series back through the query-frontend. Unlike the ingester read path (which is
	// deterministic under strong read consistency), a block becomes queryable only once the compactor has
	// written it into the per-tenant bucket index and the querier has loaded that index, both of which are
	// asynchronous: until then the query returns an empty result rather than an error. Poll until the exact
	// expected series is returned.
	for name, expectedVector := range expectedVectors {
		test.Poll(t, 30*time.Second, expectedVector, func() interface{} {
			res, err := queryClient.Query(name, now)
			if err != nil || res.Type() != model.ValVector {
				return model.Vector(nil)
			}
			return res.(model.Vector)
		})
	}

	// A query matching a single metric name is scoped to exactly one read compartment, so only that
	// compartment's store-gateway is queried.
	for rc := 0; rc < numReadCompartments; rc++ {
		name := namesByCompartment[rc][0]

		before := make([]float64, numReadCompartments)
		for i := range storeGateways {
			before[i] = storeGatewaySeriesRequests(t, storeGateways[i])
		}
		querierSumBefore, querierCountBefore := querierStoreGatewayCompartmentsHit(t, querier)

		_, err := queryClient.Query(name, now)
		require.NoErrorf(t, err, "metric: %s", name)

		// The targeted compartment's store-gateway served one more Series request...
		require.NoErrorf(t, storeGateways[rc].WaitSumMetricsWithOptions(e2e.Greater(before[rc]), []string{"cortex_request_duration_seconds"}, e2e.WithMetricCount, e2e.WithLabelMatchers(
			labels.MustNewMatcher(labels.MatchEqual, "route", storeGatewaySeriesRoute)), e2e.WaitMissingMetrics),
			"read compartment: %d", rc)

		// ...while every other compartment's store-gateway was not queried at all.
		for other := range storeGateways {
			if other == rc {
				continue
			}
			require.Equalf(t, before[other], storeGatewaySeriesRequests(t, storeGateways[other]),
				"store-gateway of read compartment %d must not be queried for a query scoped to compartment %d", other, rc)
		}

		// The querier reports exactly one read compartment queried for this query.
		querierSumAfter, querierCountAfter := querierStoreGatewayCompartmentsHit(t, querier)
		require.Equal(t, querierCountBefore+1, querierCountAfter)
		require.Equal(t, querierSumBefore+1, querierSumAfter)
	}

	// A query whose __name__ matcher can't be narrowed to a single compartment (a non-enumerable regexp)
	// is fanned out to every read compartment's store-gateway.
	{
		before := make([]float64, numReadCompartments)
		for i := range storeGateways {
			before[i] = storeGatewaySeriesRequests(t, storeGateways[i])
		}
		querierSumBefore, querierCountBefore := querierStoreGatewayCompartmentsHit(t, querier)

		_, err := queryClient.Query(`{__name__=~"compartment_series_.*"}`, now)
		require.NoError(t, err)

		for i := range storeGateways {
			require.NoErrorf(t, storeGateways[i].WaitSumMetricsWithOptions(e2e.Greater(before[i]), []string{"cortex_request_duration_seconds"}, e2e.WithMetricCount, e2e.WithLabelMatchers(
				labels.MustNewMatcher(labels.MatchEqual, "route", storeGatewaySeriesRoute)), e2e.WaitMissingMetrics),
				"read compartment: %d", i)
		}

		// The querier reports every read compartment queried for this query.
		querierSumAfter, querierCountAfter := querierStoreGatewayCompartmentsHit(t, querier)
		require.Equal(t, querierCountBefore+1, querierCountAfter)
		require.Equal(t, querierSumBefore+float64(numReadCompartments), querierSumAfter)
	}
}

// compartmentBlocksBucketNames derives, from the base blocks-storage bucket name, the placeholder template
// the querier resolves to read every read compartment's bucket, and the resolved bucket name of each read
// compartment (the compartment's ingester ships to it and the querier reads it).
func compartmentBlocksBucketNames(baseBucketName string, numReadCompartments int) (template string, buckets []string) {
	template = fmt.Sprintf("%s-rc-%s", baseBucketName, compartments.ReadCompartmentIDPlaceholder)
	buckets = make([]string, numReadCompartments)
	for rc := range buckets {
		buckets[rc] = compartments.ReplaceReadCompartment(template, rc)
	}
	return template, buckets
}

// metricNamesPerCompartment returns perCompartment distinct metric names for each read compartment,
// using the router to classify names exactly as the distributor does.
func metricNamesPerCompartment(router *compartments.Router, numReadCompartments, perCompartment int) [][]string {
	names := make([][]string, numReadCompartments)

	for i := 0; ; i++ {
		name := fmt.Sprintf("compartment_series_%d", i)
		if c := router.CompartmentForMetric(userID, name); len(names[c]) < perCompartment {
			names[c] = append(names[c], name)
		}

		complete := true
		for _, n := range names {
			if len(n) < perCompartment {
				complete = false
				break
			}
		}
		if complete {
			return names
		}
	}
}

// waitQueryFrontendToFetchOffsetsPerWriteCompartment waits until the query-frontend has successfully
// fetched the last produced offsets from each write compartment's Kafka cluster at least once. With
// compartments the offset reader emits its metrics per write_compartment, so success must be checked
// per cluster rather than in aggregate.
func waitQueryFrontendToFetchOffsetsPerWriteCompartment(t *testing.T, queryFrontend *e2emimir.MimirService, numWriteCompartments int) {
	test.Poll(t, 30*time.Second, true, func() interface{} {
		for wc := 0; wc < numWriteCompartments; wc++ {
			matcher := e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "write_compartment", strconv.Itoa(wc)))

			requests, requestsErr := queryFrontend.SumMetrics([]string{"cortex_ingest_storage_reader_last_produced_offset_request_duration_seconds"}, e2e.WithMetricCount, matcher, e2e.WaitMissingMetrics)
			failures, failuresErr := queryFrontend.SumMetrics([]string{"cortex_ingest_storage_reader_last_produced_offset_failures_total"}, matcher, e2e.WaitMissingMetrics)

			t.Logf("Waiting query-frontend to fetch last produced offsets for write compartment %d – requestsErr: %v requests: %v failuresErr: %v failures: %v", wc, requestsErr, requests, failuresErr, failures)
			if requestsErr != nil || failuresErr != nil || len(requests) != 1 || len(failures) != 1 || requests[0] <= failures[0] {
				return false
			}
		}
		return true
	})
}

// storeGatewaySeriesRoute is the gRPC route label of the store-gateway Series endpoint.
const storeGatewaySeriesRoute = "/gatewaypb.StoreGateway/Series"

// flushIngesterBlocks triggers force-compaction of the ingester's TSDB head into a block and its shipping.
// It runs asynchronously because the default 1s HTTP timeout is too short for a synchronous flush.
func flushIngesterBlocks(t *testing.T, ingester *e2emimir.MimirService) {
	res, err := e2e.DoGet("http://" + ingester.HTTPEndpoint() + "/ingester/flush")
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, res.StatusCode)
}

// storeGatewaySeriesRequests returns the number of Series requests the store-gateway has served so far.
func storeGatewaySeriesRequests(t *testing.T, storeGateway *e2emimir.MimirService) float64 {
	sums, err := storeGateway.SumMetrics([]string{"cortex_request_duration_seconds"}, e2e.WithMetricCount, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "route", storeGatewaySeriesRoute)), e2e.SkipMissingMetrics)
	require.NoError(t, err)
	return sums[0]
}

// querierStoreGatewayCompartmentsHit returns the running sum and count of the querier's
// cortex_querier_compartments_hit_per_query histogram for the store-gateway path: sum is the total number
// of read compartments queried across all queries, count is the number of queries.
func querierStoreGatewayCompartmentsHit(t *testing.T, querier *e2emimir.MimirService) (sum, count float64) {
	matcher := e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "storage", "store-gateway"))
	sums, err := querier.SumMetrics([]string{"cortex_querier_compartments_hit_per_query"}, matcher, e2e.SkipMissingMetrics)
	require.NoError(t, err)
	counts, err := querier.SumMetrics([]string{"cortex_querier_compartments_hit_per_query"}, e2e.WithMetricCount, matcher, e2e.SkipMissingMetrics)
	require.NoError(t, err)
	return sums[0], counts[0]
}
