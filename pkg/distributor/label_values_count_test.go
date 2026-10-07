// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/util/validation"
)

// labelValuesCountFixtures returns series whose labels have different cardinalities, and the expected count per label.
func labelValuesCountFixtures() ([][]string, map[string]int) {
	var series [][]string
	for i := range 120 {
		lbls := []string{labels.MetricName, fmt.Sprintf("metric_%d", i%3), "pod", fmt.Sprintf("pod-%03d", i%40), "id", fmt.Sprintf("id-%04d", i)}
		if i%2 == 0 {
			lbls = append(lbls, "env", fmt.Sprintf("env-%d", i%5))
		}
		series = append(series, lbls)
	}
	return series, map[string]int{labels.MetricName: 3, "pod": 40, "id": 120, "env": 5}
}

// prepareLabelValuesCount creates distributors with the streaming merge enabled or not, and pushes the fixtures.
func prepareLabelValuesCount(t *testing.T, cfg prepConfig, streaming bool, series [][]string) ([]*Distributor, *validation.Limits) {
	t.Helper()
	if cfg.limits == nil {
		cfg.limits = &validation.Limits{}
		flagext.DefaultValues(cfg.limits)
	}
	cfg.limits.CardinalityAnalysisStreamingMergeEnabled = streaming
	// Small messages, so that labels are split across messages. Mock ingesters don't clamp the size.
	cfg.limits.CardinalityAnalysisStreamingMergeMessageSize = 64
	cfg.numDistributors = 1

	ds, _, _, _ := prepare(t, cfg)
	ctx := labelValuesCountContext(t)
	for _, lbls := range series {
		_, err := ds[0].Push(ctx, mockWriteRequest(labelAdapters(lbls...), 1, 100_000))
		require.NoError(t, err)
	}
	return ds, cfg.limits
}

func labelValuesCountContext(t *testing.T) context.Context {
	// Strong read consistency avoids flaky tests when ingest storage is enabled.
	return api.ContextWithReadConsistencyLevel(user.InjectOrgID(t.Context(), "label-values-count"), api.ReadConsistencyStrong)
}

func TestDistributor_LabelValuesCountPerLabelName(t *testing.T) {
	series, expected := labelValuesCountFixtures()

	oldFirstIngester := func(_ string, idx int) labelNamesAndValuesBehaviour {
		if idx == 0 {
			return labelNamesAndValuesOld
		}
		return labelNamesAndValuesCurrent
	}

	tests := map[string]prepConfig{
		"classic ring, RF 3": {
			numIngesters: 6, happyIngesters: 6, replicationFactor: 3,
		},
		"classic ring, RF 3, one ingester failing": {
			numIngesters: 6, happyIngesters: 5, replicationFactor: 3,
		},
		"classic ring, zone-aware": {
			numIngesters: 6, happyIngesters: 6, replicationFactor: 3, ingesterZones: []string{"zone-a", "zone-b", "zone-c"},
		},
		"classic ring, one old ingester": {
			numIngesters: 6, happyIngesters: 6, replicationFactor: 3, labelNamesAndValuesBehaviour: oldFirstIngester,
		},
		"ingest storage": {
			numIngesters: 4, happyIngesters: 4, ingestStorageEnabled: true,
		},
		"ingest storage, replicas in two zones": {
			numIngesters: 8, happyIngesters: 8, ingesterZones: []string{"zone-a", "zone-b"}, ingestStorageEnabled: true, ingestStoragePartitions: 4,
		},
		"ingest storage, one old ingester": {
			numIngesters: 4, happyIngesters: 4, ingestStorageEnabled: true, labelNamesAndValuesBehaviour: oldFirstIngester,
		},
	}

	for name, cfg := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			for _, streaming := range []bool{false, true} {
				t.Run(fmt.Sprintf("streaming merge: %t", streaming), func(t *testing.T) {
					ds, _ := prepareLabelValuesCount(t, cfg, streaming, series)
					counts, err := ds[0].LabelValuesCountPerLabelName(labelValuesCountContext(t), nil, cardinality.InMemoryMethod)
					require.NoError(t, err)
					require.Equal(t, expected, counts)
				})
			}
		})
	}

	t.Run("with matchers", func(t *testing.T) {
		t.Parallel()
		for _, streaming := range []bool{false, true} {
			ds, _ := prepareLabelValuesCount(t, prepConfig{numIngesters: 3, happyIngesters: 3, replicationFactor: 3}, streaming, series)
			// Mock ingesters ignore matchers, but the request must still succeed with them.
			counts, err := ds[0].LabelValuesCountPerLabelName(labelValuesCountContext(t), []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "env", "env-0")}, cardinality.InMemoryMethod)
			require.NoError(t, err)
			require.Equal(t, expected, counts)
		}
	})

	t.Run("no data", func(t *testing.T) {
		t.Parallel()
		for _, streaming := range []bool{false, true} {
			ds, _ := prepareLabelValuesCount(t, prepConfig{numIngesters: 3, happyIngesters: 3, replicationFactor: 3}, streaming, nil)
			counts, err := ds[0].LabelValuesCountPerLabelName(labelValuesCountContext(t), nil, cardinality.InMemoryMethod)
			require.NoError(t, err)
			require.Empty(t, counts)
		}
	})
}

func TestDistributor_LabelValuesCountPerLabelName_Fallback(t *testing.T) {
	series, expected := labelValuesCountFixtures()
	allOld := func(string, int) labelNamesAndValuesBehaviour { return labelNamesAndValuesOld }

	t.Run("falls back to the in-memory merge when ingesters don't sort", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		ds, _ := prepareLabelValuesCount(t, prepConfig{numIngesters: 3, happyIngesters: 3, replicationFactor: 3, reg: reg, labelNamesAndValuesBehaviour: allOld}, true, series)

		counts, err := ds[0].LabelValuesCountPerLabelName(labelValuesCountContext(t), nil, cardinality.InMemoryMethod)
		require.NoError(t, err)
		require.Equal(t, expected, counts)

		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_distributor_label_names_and_values_merges_total The total number of label names cardinality merges by merge method, and whether the streaming merge fell back to the in-memory merge. A retried request counts once per attempt.
			# TYPE cortex_distributor_label_names_and_values_merges_total counter
			cortex_distributor_label_names_and_values_merges_total{fallback="true",method="map"} 1
		`), "cortex_distributor_label_names_and_values_merges_total"))
	})

	t.Run("fallback enforces the size limit", func(t *testing.T) {
		limits := &validation.Limits{}
		flagext.DefaultValues(limits)
		limits.LabelNamesAndValuesResultsMaxSizeBytes = 100
		ds, _ := prepareLabelValuesCount(t, prepConfig{numIngesters: 3, happyIngesters: 3, replicationFactor: 3, limits: limits, labelNamesAndValuesBehaviour: allOld}, true, series)

		_, err := ds[0].LabelValuesCountPerLabelName(labelValuesCountContext(t), nil, cardinality.InMemoryMethod)
		require.ErrorIs(t, err, ErrResponseTooLarge)
	})

	t.Run("streaming merge doesn't apply the size limit", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		limits := &validation.Limits{}
		flagext.DefaultValues(limits)
		limits.LabelNamesAndValuesResultsMaxSizeBytes = 100
		ds, _ := prepareLabelValuesCount(t, prepConfig{numIngesters: 3, happyIngesters: 3, replicationFactor: 3, limits: limits, reg: reg}, true, series)

		counts, err := ds[0].LabelValuesCountPerLabelName(labelValuesCountContext(t), nil, cardinality.InMemoryMethod)
		require.NoError(t, err)
		require.Equal(t, expected, counts)
		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_distributor_label_names_and_values_merges_total The total number of label names cardinality merges by merge method, and whether the streaming merge fell back to the in-memory merge. A retried request counts once per attempt.
			# TYPE cortex_distributor_label_names_and_values_merges_total counter
			cortex_distributor_label_names_and_values_merges_total{fallback="false",method="streaming"} 1
		`), "cortex_distributor_label_names_and_values_merges_total"))
	})
}

func TestDistributor_LabelValuesCountPerLabelName_StreamErrors(t *testing.T) {
	series, _ := labelValuesCountFixtures()

	t.Run("out of order stream", func(t *testing.T) {
		ds, _ := prepareLabelValuesCount(t, prepConfig{
			numIngesters: 3, happyIngesters: 3, replicationFactor: 3,
			labelNamesAndValuesBehaviour: func(string, int) labelNamesAndValuesBehaviour { return labelNamesAndValuesOutOfOrder },
		}, true, series)

		_, err := ds[0].LabelValuesCountPerLabelName(labelValuesCountContext(t), nil, cardinality.InMemoryMethod)
		require.ErrorIs(t, err, errLabelValuesOutOfOrder)
	})

	for name, behaviour := range map[string]labelNamesAndValuesBehaviour{
		"ingester goes down":                             labelNamesAndValuesFailOnceThenUnavailable,
		"ingester keeps failing after the first message": labelNamesAndValuesFailAfterFirstMessage,
	} {
		t.Run("stream fails after quorum, and the retry excludes the ingester: "+name, func(t *testing.T) {
			_, expected := labelValuesCountFixtures()
			reg := prometheus.NewPedanticRegistry()
			ds, _ := prepareLabelValuesCount(t, prepConfig{
				numIngesters: 3, happyIngesters: 3, replicationFactor: 3, reg: reg,
				configure: func(cfg *Config) { cfg.MinimizeIngesterRequests = false },
				// Ingester 1 holds its first message until ingester 0 fails, so quorum is always ingesters 0 and 2.
				labelNamesAndValuesGate: newLabelNamesAndValuesGate(),
				labelNamesAndValuesBehaviour: func(_ string, idx int) labelNamesAndValuesBehaviour {
					switch idx {
					case 0:
						return behaviour
					case 1:
						return labelNamesAndValuesCurrentAfterFailure
					default:
						return labelNamesAndValuesCurrent
					}
				},
			}, true, series)

			counts, err := ds[0].LabelValuesCountPerLabelName(labelValuesCountContext(t), nil, cardinality.InMemoryMethod)
			require.NoError(t, err)
			require.Equal(t, expected, counts)
			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
				# HELP cortex_distributor_label_names_and_values_streaming_retries_total The total number of label names cardinality requests that the streaming merge retried because an ingester stream failed after quorum.
				# TYPE cortex_distributor_label_names_and_values_streaming_retries_total counter
				cortex_distributor_label_names_and_values_streaming_retries_total 1
			`), "cortex_distributor_label_names_and_values_streaming_retries_total"))
		})
	}

	t.Run("failing ingesters in a zone-aware ring", func(t *testing.T) {
		_, expected := labelValuesCountFixtures()
		ds, _ := prepareLabelValuesCount(t, prepConfig{
			numIngesters: 6, happyIngesters: 6, replicationFactor: 3, ingesterZones: []string{"zone-a", "zone-b", "zone-c"},
			labelNamesAndValuesBehaviour: func(zone string, idx int) labelNamesAndValuesBehaviour {
				if zone == "zone-a" && idx == 0 {
					return labelNamesAndValuesFailAfterFirstMessage
				}
				return labelNamesAndValuesCurrent
			},
		}, true, series)

		counts, err := ds[0].LabelValuesCountPerLabelName(labelValuesCountContext(t), nil, cardinality.InMemoryMethod)
		require.NoError(t, err)
		require.Equal(t, expected, counts)
	})

	t.Run("a failing zone across partitions costs one retry", func(t *testing.T) {
		_, expected := labelValuesCountFixtures()
		reg := prometheus.NewPedanticRegistry()
		ds, _ := prepareLabelValuesCount(t, prepConfig{
			numIngesters: 8, happyIngesters: 8, ingesterZones: []string{"zone-a", "zone-b"}, ingestStorageEnabled: true, ingestStoragePartitions: 4, reg: reg,
			// Every partition queries zone-a first, where every ingester fails after its first message.
			configure: func(cfg *Config) {
				cfg.MinimizeIngesterRequests = true
				cfg.PreferAvailabilityZones = []string{"zone-a"}
			},
			labelNamesAndValuesBehaviour: func(zone string, _ int) labelNamesAndValuesBehaviour {
				if zone == "zone-a" {
					return labelNamesAndValuesFailAfterFirstMessage
				}
				return labelNamesAndValuesCurrent
			},
		}, true, series)

		counts, err := ds[0].LabelValuesCountPerLabelName(labelValuesCountContext(t), nil, cardinality.InMemoryMethod)
		require.NoError(t, err)
		require.Equal(t, expected, counts)
		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_distributor_label_names_and_values_streaming_retries_total The total number of label names cardinality requests that the streaming merge retried because an ingester stream failed after quorum.
			# TYPE cortex_distributor_label_names_and_values_streaming_retries_total counter
			cortex_distributor_label_names_and_values_streaming_retries_total 1
		`), "cortex_distributor_label_names_and_values_streaming_retries_total"))
	})

	t.Run("retries stop at the limit even with error budget left", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		ds, _ := prepareLabelValuesCount(t, prepConfig{
			// RF 7 tolerates 3 failing ingesters, one more than the retry limit.
			numIngesters: 7, happyIngesters: 7, replicationFactor: 7, reg: reg,
			configure:                    func(cfg *Config) { cfg.MinimizeIngesterRequests = false },
			labelNamesAndValuesBehaviour: func(string, int) labelNamesAndValuesBehaviour { return labelNamesAndValuesFailAfterFirstMessage },
		}, true, series)

		_, err := ds[0].LabelValuesCountPerLabelName(labelValuesCountContext(t), nil, cardinality.InMemoryMethod)
		require.ErrorIs(t, err, errFail)
		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
			# HELP cortex_distributor_label_names_and_values_streaming_retries_total The total number of label names cardinality requests that the streaming merge retried because an ingester stream failed after quorum.
			# TYPE cortex_distributor_label_names_and_values_streaming_retries_total counter
			cortex_distributor_label_names_and_values_streaming_retries_total %d
		`, maxLabelValuesCountStreamingRetries)), "cortex_distributor_label_names_and_values_streaming_retries_total"))
	})

	t.Run("stream fails after quorum on every attempt", func(t *testing.T) {
		ds, _ := prepareLabelValuesCount(t, prepConfig{
			numIngesters: 3, happyIngesters: 3, replicationFactor: 3,
			labelNamesAndValuesBehaviour: func(string, int) labelNamesAndValuesBehaviour { return labelNamesAndValuesFailAfterFirstMessage },
		}, true, series)

		_, err := ds[0].LabelValuesCountPerLabelName(labelValuesCountContext(t), nil, cardinality.InMemoryMethod)
		require.ErrorIs(t, err, errFail)
		var streamErr *labelNamesAndValuesStreamError
		require.False(t, errors.As(err, &streamErr), "the retry marker must not leak to callers")
	})
}

func TestExcludeFailedReplica(t *testing.T) {
	a1 := ring.InstanceDesc{Addr: "a-1", Zone: "a"}
	a2 := ring.InstanceDesc{Addr: "a-2", Zone: "a"}
	b1 := ring.InstanceDesc{Addr: "b-1", Zone: "b"}
	c1 := ring.InstanceDesc{Addr: "c-1", Zone: "c"}

	t.Run("without zone awareness, excludes the instance and uses up one error", func(t *testing.T) {
		sets := []ring.ReplicationSet{{Instances: []ring.InstanceDesc{a1, a2, b1}, MaxErrors: 1}}
		out, ok := excludeFailedReplica(sets, a1)
		require.True(t, ok)
		require.Equal(t, []ring.ReplicationSet{{Instances: []ring.InstanceDesc{a2, b1}, MaxErrors: 0}}, out)
		require.Len(t, sets[0].Instances, 3, "the input must not be modified")

		_, ok = excludeFailedReplica(out, a2)
		require.False(t, ok, "no error budget left")
	})

	t.Run("with zone awareness, excludes the instance's zone and uses up one zone", func(t *testing.T) {
		sets := []ring.ReplicationSet{{Instances: []ring.InstanceDesc{a1, a2, b1, c1}, MaxUnavailableZones: 1, ZoneAwarenessEnabled: true}}
		out, ok := excludeFailedReplica(sets, a1)
		require.True(t, ok)
		require.Equal(t, []ring.ReplicationSet{{Instances: []ring.InstanceDesc{b1, c1}, MaxUnavailableZones: 0, ZoneAwarenessEnabled: true}}, out)

		_, ok = excludeFailedReplica(out, b1)
		require.False(t, ok, "no zone budget left")
	})

	t.Run("with zone awareness, excludes the zone from every set with zone budget", func(t *testing.T) {
		b2 := ring.InstanceDesc{Addr: "b-2", Zone: "b"}
		sets := []ring.ReplicationSet{
			// One set per partition, as with ingest storage.
			{Instances: []ring.InstanceDesc{a1, b1}, MaxUnavailableZones: 1, ZoneAwarenessEnabled: true},
			{Instances: []ring.InstanceDesc{a2, b2}, MaxUnavailableZones: 1, ZoneAwarenessEnabled: true},
			// No zone budget left, so it keeps zone a.
			{Instances: []ring.InstanceDesc{a2, c1}, MaxUnavailableZones: 0, ZoneAwarenessEnabled: true},
			// No instance in zone a.
			{Instances: []ring.InstanceDesc{b1, c1}, MaxUnavailableZones: 1, ZoneAwarenessEnabled: true},
		}
		out, ok := excludeFailedReplica(sets, a1)
		require.True(t, ok)
		require.Equal(t, []ring.ReplicationSet{
			{Instances: []ring.InstanceDesc{b1}, MaxUnavailableZones: 0, ZoneAwarenessEnabled: true},
			{Instances: []ring.InstanceDesc{b2}, MaxUnavailableZones: 0, ZoneAwarenessEnabled: true},
			sets[2],
			sets[3],
		}, out)
	})

	t.Run("without zone awareness, only changes the sets that contain the instance", func(t *testing.T) {
		sets := []ring.ReplicationSet{
			{Instances: []ring.InstanceDesc{a1, b1}, MaxErrors: 1},
			{Instances: []ring.InstanceDesc{a2, c1}, MaxErrors: 1},
		}
		out, ok := excludeFailedReplica(sets, a1)
		require.True(t, ok)
		require.Equal(t, []ring.ReplicationSet{{Instances: []ring.InstanceDesc{b1}, MaxErrors: 0}, sets[1]}, out)
	})

	t.Run("unknown instance", func(t *testing.T) {
		_, ok := excludeFailedReplica([]ring.ReplicationSet{{Instances: []ring.InstanceDesc{a1}, MaxErrors: 1}}, b1)
		require.False(t, ok)
	})
}

func TestMergeLabelNamesAndValuesStreamsInMemory_KeepsFirstError(t *testing.T) {
	// The first stream's message exceeds the size limit. The second stream blocks until it's cancelled, and its
	// cancellation returns only once its goroutine has had time to report the cancellation error first.
	tooLarge := &client.LabelNamesAndValuesResponse{Items: []*client.LabelValues{{LabelName: "name", Values: []string{strings.Repeat("v", 100)}}}}
	blocked := &cancelBlockingLabelNamesAndValuesStream{unblock: make(chan struct{}), returned: make(chan struct{})}
	streams := []*labelNamesAndValuesStream{
		{instance: ring.InstanceDesc{Addr: "too-large"}, stream: &labelNamesAndValuesMockStream{}, cancel: func(error) {}, first: tooLarge},
		{instance: ring.InstanceDesc{Addr: "blocked"}, stream: blocked, cancel: blocked.cancel, first: &client.LabelNamesAndValuesResponse{}},
	}

	_, err := mergeLabelNamesAndValuesStreamsInMemory(streams, 10)
	require.ErrorIs(t, err, ErrResponseTooLarge)
}

// cancelBlockingLabelNamesAndValuesStream blocks Recv until cancel is called. cancel waits until Recv has returned
// the cancellation error, so the stream's goroutine is likely to finish before the one that called cancel.
type cancelBlockingLabelNamesAndValuesStream struct {
	grpc.ClientStream
	// cancelled is set by the first cancel call. Later calls return at once, unlike sync.Once, which would block
	// them until the first call returns and let the other goroutine report its error first.
	cancelled atomic.Bool
	unblock   chan struct{}
	returned  chan struct{}
}

func (s *cancelBlockingLabelNamesAndValuesStream) Recv() (*client.LabelNamesAndValuesResponse, error) {
	<-s.unblock
	defer close(s.returned)
	return nil, context.Canceled
}

func (s *cancelBlockingLabelNamesAndValuesStream) cancel(error) {
	if !s.cancelled.CompareAndSwap(false, true) {
		return
	}
	close(s.unblock)
	<-s.returned
	time.Sleep(50 * time.Millisecond)
}

func TestDistributor_LabelValuesCountPerLabelName_NoGoroutineLeak(t *testing.T) {
	series, expected := labelValuesCountFixtures()
	behaviours := map[string]struct {
		behaviour   labelNamesAndValuesBehaviour
		expectedErr error
	}{
		"sorted":               {behaviour: labelNamesAndValuesCurrent},
		"fallback":             {behaviour: labelNamesAndValuesOld},
		"out of order":         {behaviour: labelNamesAndValuesOutOfOrder, expectedErr: errLabelValuesOutOfOrder},
		"fail after first msg": {behaviour: labelNamesAndValuesFailAfterFirstMessage, expectedErr: errFail},
	}
	for name, tc := range behaviours {
		t.Run(name, func(t *testing.T) {
			ds, _ := prepareLabelValuesCount(t, prepConfig{
				numIngesters: 3, happyIngesters: 3, replicationFactor: 3,
				labelNamesAndValuesBehaviour: func(string, int) labelNamesAndValuesBehaviour { return tc.behaviour },
			}, true, series)

			// Only goroutines started by the request count, not the distributor's own.
			ignoreCurrent := goleak.IgnoreCurrent()
			counts, err := ds[0].LabelValuesCountPerLabelName(labelValuesCountContext(t), nil, cardinality.InMemoryMethod)
			if tc.expectedErr == nil {
				require.NoError(t, err)
				require.Equal(t, expected, counts)
			} else {
				require.ErrorIs(t, err, tc.expectedErr)
			}
			goleak.VerifyNone(t, ignoreCurrent)
		})
	}
}
