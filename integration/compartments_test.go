// SPDX-License-Identifier: AGPL-3.0-only

package integration

import (
	"fmt"
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
	minio := e2edb.NewMinio(9000, baseFlags["-blocks-storage.s3.bucket-name"])
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
			"-ingester.read-compartment-id": strconv.Itoa(rc),
		}))
	}

	queryFrontend := e2emimir.NewQueryFrontend("query-frontend", consul.NetworkHTTPEndpoint(), baseFlags)
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), baseFlags)

	mimirServices := []e2e.Service{queryFrontend, querier}
	for _, d := range distributors {
		mimirServices = append(mimirServices, d)
	}
	for _, i := range ingesters {
		mimirServices = append(mimirServices, i)
	}
	require.NoError(t, s.StartAndWaitReady(mimirServices...))

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
