// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

// Metadata is decided by each metric's own hash, so a profile with it generates the same series as one without.
func TestMetadataLeavesTheWorldAsItWas(t *testing.T) {
	without := newWorld(defaultProfile().Series, 20_000, 1)
	p := defaultProfile().Series
	p.MetadataShare = 0.5
	with := newWorld(p, 20_000, 1)
	require.Equal(t, len(without.tenants), len(with.tenants))
	some, none := 0, 0
	for i, tenant := range with.tenants {
		require.Equal(t, len(without.tenants[i].metrics), len(tenant.metrics))
		for k, m := range tenant.metrics {
			require.Equal(t, without.tenants[i].metrics[k].seriesLabels(0, 0, ""), m.seriesLabels(0, 0, ""))
			require.Nil(t, without.tenants[i].metrics[k].metadata)
			if m.metadata == nil {
				none++
			} else {
				some++
			}
		}
	}
	require.InDelta(t, 0.5, float64(some)/float64(some+none), 0.1)
}

func TestSeriesMetadataFollowsTheProfile(t *testing.T) {
	p := defaultProfile().Series
	p.MetadataShare, p.HelpLength, p.MetadataChurnPerHour = 1, Percentiles{"0": 50, "100": 50}, 1
	w := newWorld(p, 2_000, 1)
	m := w.tenants[0].metrics[0]
	value := w.seriesMetadata(m, 0, 0)
	require.False(t, value.IsEmpty())
	require.Len(t, value.Help, 50+len(" (0)"))
	require.NotEqual(t, value, w.seriesMetadata(m, 0, 3_600_000), "a churning metric's help changes every hour")
	require.Equal(t, value, w.seriesMetadata(m, 1, 0), "the slots of a metric share it")
	w.profile.UniqueHelp = true
	require.NotEqual(t, w.seriesMetadata(m, 0, 0), w.seriesMetadata(m, 1, 0), "unless their help is their own")
}

// The feed's records carry each series' own metadata, and their families' as stand-ins, as a distributor writes them.
func TestFeedWritesSeriesAndFamilyMetadata(t *testing.T) {
	p := defaultProfile().Series
	p.MetadataShare, p.SeriesPerRequest = 1, 100
	w := newWorld(p, 2_000, 1)
	f, err := newFeed(w, "", 0)
	require.NoError(t, err)
	t.Cleanup(f.close)
	second := time.Now().UnixMilli() / 1000 * 1000
	require.NoError(t, f.produceSecond(context.Background(), second, true))

	withData, standIns := 0, 0
	consumeRecords(t, f, func(_ *kgo.Record, req *mimirpb.WriteRequest) {
		require.LessOrEqual(t, len(req.Timeseries)-len(req.Metadata), p.SeriesPerRequest)
		families := map[string]bool{}
		for _, m := range req.Metadata {
			families[m.MetricFamilyName] = true
		}
		for _, ts := range req.Timeseries {
			if len(ts.Samples) == 0 && len(ts.Histograms) == 0 {
				standIns++
				continue
			}
			withData++
			require.False(t, ts.SeriesMetadata.IsEmpty())
		}
		require.NotEmpty(t, families)
	})
	require.Positive(t, withData)
	require.Positive(t, standIns)
}

// The feed splits each request over the tenant's partitions as a distributor does: by series, and each family's
// metadata by family.
func TestFeedSplitsRequestsOverTheTenantsPartitions(t *testing.T) {
	p := defaultProfile().Series
	p.MetadataShare, p.SeriesPerRequest, p.SeriesPerTarget, p.PartitionsPerTenant = 1, 100, 4, 3
	w := newWorld(p, 2_000, 1)
	f, err := newFeed(w, "", 0)
	require.NoError(t, err)
	t.Cleanup(f.close)
	second := time.Now().UnixMilli() / 1000 * 1000
	require.NoError(t, f.produceSecond(context.Background(), second, true))

	withData := 0
	consumeRecords(t, f, func(record *kgo.Record, req *mimirpb.WriteRequest) {
		tenant := string(record.Key)
		partitions := map[uint32]bool{}
		for _, ts := range req.Timeseries {
			if len(ts.Samples) > 0 || len(ts.Histograms) > 0 {
				withData++
				partitions[mimirpb.ShardByAllLabelAdapters(tenant, ts.Labels)%3] = true
			}
		}
		for _, m := range req.Metadata {
			partitions[mimirpb.ShardByMetricName(tenant, m.MetricFamilyName)%3] = true
		}
		require.Len(t, partitions, 1, "a record's series and families are one partition's")
		require.LessOrEqual(t, len(req.Timeseries)-len(req.Metadata), p.SeriesPerRequest)
	})
	require.Positive(t, withData)
}

// consumeRecords calls f with each record the feed produced, and the request it holds.
func consumeRecords(t *testing.T, feed *feed, f func(*kgo.Record, *mimirpb.WriteRequest)) {
	consumer, err := kgo.NewClient(kgo.SeedBrokers(feed.address()), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	require.NoError(t, err)
	t.Cleanup(consumer.Close)
	for consumed := int64(0); consumed <= feed.lastOffset.Load(); {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		fetches := consumer.PollFetches(ctx)
		cancel()
		require.NoError(t, fetches.Err())
		fetches.EachRecord(func(record *kgo.Record) {
			consumed++
			var req mimirpb.PreallocWriteRequest
			require.NoError(t, ingest.DeserializeRecordContent(record.Value, &req, ingest.ParseRecordVersion(record)))
			f(record, &req.WriteRequest)
		})
	}
}

// The threshold profile's records are made of what its sample of real partitions' records is, at the bench's size.
func TestThresholdProfileHasItsComposition(t *testing.T) {
	profile, err := loadProfile("native-metadata/threshold.json")
	require.NoError(t, err)
	require.NotNil(t, profile.Series.Composition)
	f, err := newFeed(newWorld(profile.Series, 1_650_000, 1), "", 0)
	require.NoError(t, err)
	t.Cleanup(f.close)
	// A third of the profile's scrape interval, whose seconds are alike.
	start := time.Now().UnixMilli()/60_000*60_000 - 3_600_000
	require.NoError(t, f.produceSeconds(context.Background(), start, start+20_000))

	report := validReport(0, "x-prometheus", "y-prometheus", 1, 1)
	report.Composition, report.CompositionTarget = f.Composition(), profile.Series.Composition
	require.Greater(t, report.Composition.Records, 200)
	require.Empty(t, analyzeRun("run", report).Discard)
}
