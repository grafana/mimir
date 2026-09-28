// SPDX-License-Identifier: AGPL-3.0-only

package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/grafana/dskit/ring"
	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
	"github.com/grafana/mimir/pkg/ingester"
	"github.com/grafana/mimir/pkg/mimirpb"
)

const (
	derivedTokensNumPartitions  = 2
	derivedTokensShuffledUserID = "e2e-user-shuffled"
)

// TestIngestStorage_DerivedPartitionTokens runs an ingest storage cluster that gossips its rings over
// memberlist, with ingesters that create their partitions with derived tokens.
func TestIngestStorage_DerivedPartitionTokens(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, writeFileToSharedDir(s, "runtime.yaml", []byte(fmt.Sprintf(`
overrides:
  %s:
    ingestion_partitions_tenant_shard_size: 1
`, derivedTokensShuffledUserID))))

	flags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
		IngestStorageFlags(e2edb.KafkaAuthNone),
		map[string]string{
			"-runtime-config.file": filepath.Join(e2e.ContainerSharedDir, "runtime.yaml"),

			// Gossip every ring, so that the partition ring is exchanged through memberlist.
			"-ingester.ring.store":               "memberlist",
			"-ingester.partition-ring.store":     "memberlist",
			"-distributor.ring.store":            "memberlist",
			"-querier.ring.store":                "memberlist",
			"-store-gateway.sharding-ring.store": "memberlist",
			"-memberlist.bind-port":              "8000",

			"-partition-ring.max-derived-token-partitions": strconv.Itoa(derivedTokensNumPartitions),
		},
	)
	ingesterFlags := map[string]string{"-ingester.partition-ring.create-partitions-with-derived-tokens": "true"}

	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	kafka := e2edb.NewKafka()
	require.NoError(t, s.StartAndWaitReady(minio, kafka))

	// The first ingester seeds the memberlist cluster. The trailing "-0" of the name sets the partition ID.
	ingester0 := e2emimir.NewIngester("ingester-0", "", mergeFlags(flags, ingesterFlags, map[string]string{
		"-ingester.ring.observe-period": "0s",
	}))
	require.NoError(t, s.StartAndWaitReady(ingester0))

	joinFlags := mergeFlags(flags, map[string]string{
		"-memberlist.join":              networkName + "-ingester-0:8000",
		"-ingester.ring.observe-period": "5s",
	})
	ingester1 := e2emimir.NewIngester("ingester-1", "", mergeFlags(joinFlags, ingesterFlags))
	distributor := e2emimir.NewDistributor("distributor", "", joinFlags)
	querier := e2emimir.NewQuerier("querier", "", joinFlags)
	require.NoError(t, s.StartAndWaitReady(ingester1, distributor, querier))

	ingesters := []*e2emimir.MimirService{ingester0, ingester1}
	components := []*e2emimir.MimirService{ingester0, ingester1, distributor, querier}
	for _, svc := range components {
		require.NoError(t, svc.WaitSumMetrics(e2e.Equals(float64(len(components))), "memberlist_client_cluster_members_count"), svc.Name())
		require.NoError(t, svc.WaitSumMetricsWithOptions(e2e.Equals(derivedTokensNumPartitions), []string{"cortex_partition_ring_partitions"},
			e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "state", "Active"))), svc.Name())
	}

	storedRing, err := ring.NewPartitionRing(*storedPartitionRingDesc())
	require.NoError(t, err)

	for _, svc := range components {
		// The gossiped value holds no tokens.
		for id, partition := range partitionRingKVValue(t, svc) {
			assert.Equal(t, ring.PartitionTokensSmt512, partition.TokenScheme, "%s: partition %d", svc.Name(), id)
			assert.Empty(t, partition.Tokens, "%s: partition %d", svc.Name(), id)
		}
	}

	shuffledRing, err := storedRing.ShuffleShard(derivedTokensShuffledUserID, 1)
	require.NoError(t, err)

	for tenantID, tenantRing := range map[string]*ring.PartitionRing{userID: storedRing, derivedTokensShuffledUserID: shuffledRing} {
		client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", tenantID)
		require.NoError(t, err)

		now := time.Now()
		expectedSeriesPerPartition := map[int32]int{}
		expectedVectors := map[string]model.Vector{}
		for i := 0; i < 50; i++ {
			metricName := fmt.Sprintf("series_%d", i)
			series, expectedVector, _ := generateFloatSeries(metricName, now)

			res, err := client.Push(series)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, res.StatusCode)
			expectedVectors[metricName] = expectedVector

			partitionID, err := tenantRing.ActivePartitionForKey(mimirpb.ShardByAllLabels(tenantID, labels.FromStrings(model.MetricNameLabel, metricName)))
			require.NoError(t, err)
			expectedSeriesPerPartition[partitionID]++
		}

		for metricName, expectedVector := range expectedVectors {
			result, _, _, err := client.Query(metricName, now)
			require.NoError(t, err, metricName)
			require.Equal(t, model.ValVector, result.Type(), metricName)
			assert.Equal(t, expectedVector, result.(model.Vector), metricName)
		}

		// Only the right tokens send each series to the partition the local ring predicts. Wait for every
		// series to be consumed first, so a partition expected to be empty can't pass before a misrouted
		// series arrives.
		tenantMatcher := e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "user", tenantID))
		require.NoError(t, e2emimir.NewCompositeMimirService(ingesters...).WaitSumMetricsWithOptions(e2e.Equals(float64(len(expectedVectors))), []string{"cortex_ingester_memory_series_created_total"},
			tenantMatcher, e2e.SkipMissingMetrics), "tenant %s", tenantID)
		for id, svc := range ingesters {
			series, err := svc.SumMetrics([]string{"cortex_ingester_memory_series_created_total"}, tenantMatcher, e2e.SkipMissingMetrics)
			require.NoError(t, err)
			assert.Equal(t, float64(expectedSeriesPerPartition[int32(id)]), series[0], "tenant %s, partition %d", tenantID, id)
		}
	}
}

// storedPartitionRingDesc returns a desc of active partitions with stored tokens.
func storedPartitionRingDesc() *ring.PartitionRingDesc {
	desc := ring.NewPartitionRingDesc()
	for id := int32(0); id < derivedTokensNumPartitions; id++ {
		desc.AddPartition(id, ring.PartitionActive, time.Now())
	}
	return desc
}

type partitionTokensView struct {
	TokenScheme ring.PartitionTokenScheme
	Tokens      []uint32
}

// partitionRingKVValue returns the partitions of the ingester partition ring value that svc holds in memberlist.
func partitionRingKVValue(t *testing.T, svc *e2emimir.MimirService) map[int32]partitionTokensView {
	t.Helper()

	var desc struct {
		Partitions map[int32]struct {
			TokenScheme ring.PartitionTokenScheme `json:"tokenScheme"`
			Tokens      []uint32                  `json:"tokens"`
		} `json:"partitions"`
	}
	query := url.Values{"viewKey": {"collectors/" + ingester.PartitionRingKey}, "format": {"json"}}
	getJSON(t, fmt.Sprintf("http://%s/memberlist?%s", svc.HTTPEndpoint(), query.Encode()), &desc)

	partitions := make(map[int32]partitionTokensView, len(desc.Partitions))
	for id, p := range desc.Partitions {
		partitions[id] = partitionTokensView{TokenScheme: p.TokenScheme, Tokens: p.Tokens}
	}
	require.Len(t, partitions, derivedTokensNumPartitions, svc.Name())
	return partitions
}

func getJSON(t *testing.T, endpoint string, v any) {
	t.Helper()

	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	require.NoError(t, err)
	req.Header.Set("Accept", "application/json")

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { require.NoError(t, res.Body.Close()) }()
	require.Equal(t, http.StatusOK, res.StatusCode, endpoint)
	require.NoError(t, json.NewDecoder(res.Body).Decode(v), endpoint)
}
