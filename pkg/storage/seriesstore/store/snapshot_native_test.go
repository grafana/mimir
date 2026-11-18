// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// nativeSeriesLabels are a series of the target with job api and instance 1, which native info() finds it by.
func nativeSeriesLabels(pod string) promlabels.Labels {
	return promlabels.FromStrings(model.MetricNameLabel, "requests_total", "job", "api", "instance", "1", "pod", pod)
}

// createSeries creates the series with a float at at, without metadata or resource.
func createSeries(t *testing.T, e *Engine, at int64, series ...promlabels.Labels) {
	app := e.Appender(t.Context())
	for _, l := range series {
		_, err := app.Append(0, l, 0, at, 1, nil, nil, storage.AppendV2Options{})
		require.NoError(t, err)
	}
	require.NoError(t, app.Commit())
}

// appendNative appends a float of value 1 at at to each of the series, which exist, as a decoded batch that has the
// metadata m and, unless attributes is nil, a resource of the attributes: the engine keeps either only while its switch
// is on, as the ingester's batches have it. It returns errors rather than failing the test, for goroutines.
func appendNative(e *Engine, at int64, m mimirpb.SeriesMetadata, attributes []mimirpb.ResourceAttribute, series ...promlabels.Labels) error {
	var resource *mimirpb.ResourceEntry
	if attributes != nil {
		resource = &mimirpb.ResourceEntry{Attributes: attributes}
	}
	batch := make([]mimirpb.PreallocTimeseries, 0, len(series))
	indices := make([]int, 0, len(series))
	for i, l := range series {
		batch = append(batch, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
			Labels: mimirpb.FromLabelsToLabelAdapters(l), Samples: []mimirpb.Sample{{TimestampMs: at, Value: 1}}, SeriesMetadata: m, Resource: resource,
		}})
		indices = append(indices, i)
	}
	leftover, _, err := e.AppendFloats(batch, indices, 0, 1<<62, 0, true, &rw2Sink{})
	if err == nil && len(leftover) > 0 {
		err = fmt.Errorf("%d of the series aren't the engine's", len(leftover))
	}
	return err
}

func counterMetadata(help string) mimirpb.SeriesMetadata {
	return mimirpb.SeriesMetadata{Type: mimirpb.COUNTER, Help: help}
}

// nativeLookups is what an engine's native metadata answers: the ingester's lookups, native info()'s lookup by target,
// the reverse index's series by attribute, the coverage starts and the stats.
type nativeLookups struct {
	metadata            map[string]NativeMetricMetadataSeries
	resources, byTarget map[string]NativeResourcesSeries
	// The series by "key=value" of each attribute looked up.
	byAttribute                 map[string][]string
	metadataFrom, resourcesFrom int64
	metadataStats               MetricMetadataStats
	resourceStats               ResourceStats
}

// lookupNative takes every lookup of the engine's native metadata, with the reverse index's of each attribute.
func lookupNative(t *testing.T, e *Engine, attributes ...mimirpb.ResourceAttribute) nativeLookups {
	all := [][]*promlabels.Matcher{{promlabels.MustNewMatcher(promlabels.MatchRegexp, model.MetricNameLabel, ".+")}}
	target := [][]*promlabels.Matcher{{promlabels.MustNewMatcher(promlabels.MatchEqual, "job", "api"), promlabels.MustNewMatcher(promlabels.MatchEqual, "instance", "1")}}
	out := nativeLookups{metadata: map[string]NativeMetricMetadataSeries{}, resources: map[string]NativeResourcesSeries{}, byTarget: map[string]NativeResourcesSeries{}, byAttribute: map[string][]string{}}
	metadataSeries, _, err := e.NativeMetricMetadata(t.Context(), all, 0)
	require.NoError(t, err)
	for _, s := range metadataSeries {
		out.metadata[s.Labels.String()] = s
	}
	resources := func(into map[string]NativeResourcesSeries, matchers [][]*promlabels.Matcher) {
		found, _, err := e.NativeResources(t.Context(), matchers, math.MinInt64, math.MaxInt64, 0)
		require.NoError(t, err)
		for _, s := range found {
			into[s.Labels.String()] = s
		}
	}
	resources(out.resources, all)
	resources(out.byTarget, target)
	var builder promlabels.ScratchBuilder
	for _, a := range attributes {
		var series []string
		for _, ref := range e.resources.seriesWithAttribute(a).ToArray() {
			index := refShard(ref)
			e.store.shards[index].RLock()
			if entry, ok := e.lookupLocked(e.tenant(index), ref); ok {
				series = append(series, toPromLabels(entry.labels, &builder).String())
			}
			e.store.shards[index].RUnlock()
		}
		slices.Sort(series)
		out.byAttribute[a.Key+"="+a.StringValue] = series
	}
	out.metadataFrom, out.resourcesFrom = e.NativeMetricMetadataCoverageStart(), e.NativeResourcesCoverageStart()
	// The counters count what happened since the process started, as any Prometheus counter does, which a restart
	// starts again; the entries' room is the name groups', not native metadata's.
	out.metadataStats = e.MetricMetadataStats()
	out.metadataStats.EntryCapacity = 0
	out.metadataStats.Oversized, out.metadataStats.Clamped, out.metadataStats.MemoHits, out.metadataStats.MemoMisses, out.metadataStats.Coalesced, out.metadataStats.Evictions = 0, 0, 0, 0, 0, 0
	out.resourceStats = e.ResourceStats()
	out.resourceStats.CoalescedIdentityChanges, out.resourceStats.CoalescedVersions, out.resourceStats.Refused, out.resourceStats.Clamped, out.resourceStats.MemoHits, out.resourceStats.MemoMisses, out.resourceStats.Evictions = 0, 0, 0, 0, 0, 0, 0
	return out
}

func lastResourceVersion(t *testing.T, lookups nativeLookups, series promlabels.Labels) ResourceVersion {
	versions := lookups.resources[series.String()].Versions
	require.NotEmpty(t, versions, series)
	return versions[len(versions)-1]
}

// A clean restart keeps the engine's native metadata: the reopened engine answers every lookup as one that never
// restarted, and goes on from where it was. A change in a window that was open at the restart is limited as on an engine
// that didn't restart, which the zones' agreement needs: budgets are counted in sample time so that zones agree.
func TestOpenEngine_RestoresNativeMetadataAfterClose(t *testing.T) {
	changed, coalesced, truncated, newcomer := nativeSeriesLabels("changed"), nativeSeriesLabels("coalesced"), nativeSeriesLabels("truncated"), nativeSeriesLabels("newcomer")
	opts := EngineOptions{Shards: 2, SecondaryHashFunction: secondaryHash, IndexedResourceAttributes: []string{"host.name"}}
	open := func(dir string) *Engine {
		e, err := OpenEngine(dir, "tenant", opts)
		require.NoError(t, err)
		// As the ingester sets them for every batch: in windows of 10s, a series' metadata and identity may change
		// once, an identity may have one new version, and three identities may be new.
		e.SetNativeMetricMetadata(true)
		e.SetNativeResources(true)
		e.SetMetricMetadataChangeLimit(1, 10_000)
		e.SetResourceLimits(ResourceLimits{WindowMs: 10_000, MaxIdentityChangesPerSeries: 1, MaxVersionsPerIdentity: 1, MaxNewIdentities: 3})
		return e
	}
	type step struct {
		at            int64
		series        promlabels.Labels
		help          string
		service, host string
	}
	apply := func(e *Engine, steps ...step) {
		for _, s := range steps {
			require.NoError(t, appendNative(e, s.at, counterMetadata(s.help), testResourceAttributes(s.service, s.host), s.series))
		}
	}
	stream := func(e *Engine) {
		createSeries(t, e, 0, changed, coalesced, truncated)
		apply(e,
			step{1_000, changed, "h1", "a", "node-1"},
			step{1_000, coalesced, "h1", "a", "node-1"},
			// A metadata change and an identity change, each the one its window lets through, and a version of a.
			step{2_000, changed, "h2", "b", "node-1"},
			step{2_000, coalesced, "h2", "a", "node-2"},
			// Over the limits: a coalesced metadata run, and a version of a marked unknown, both open until 10s.
			step{3_000, coalesced, "h3", "a", "node-3"},
			// The third new identity.
			step{3_000, truncated, "t", "d", "node-1"},
		)
		// One metadata change per window, which the limit lets through, until the series is past the cap on its runs.
		for n := range maxMetadataRunsPerSeries + 2 {
			require.NoError(t, appendNative(e, int64(n+1)*10_000, counterMetadata(fmt.Sprint("t", n)), nil, truncated))
		}
	}
	attributes := []mimirpb.ResourceAttribute{{Key: "service.name", StringValue: "a"}, {Key: "host.name", StringValue: "node-2"}}

	dir := t.TempDir()
	restarted := open(dir)
	stream(restarted)
	require.NoError(t, restarted.Close())
	restarted = open(dir)
	t.Cleanup(func() { _ = restarted.Close() })
	require.True(t, restarted.Restored())
	unstopped := open(t.TempDir())
	t.Cleanup(func() { _ = unstopped.Close() })
	stream(unstopped)

	expected := lookupNative(t, unstopped, attributes...)
	// What the restart has to keep, which the stream would otherwise not test.
	require.True(t, expected.metadata[coalesced.String()].Coalesced)
	require.True(t, expected.metadata[truncated.String()].Truncated)
	require.True(t, lastResourceVersion(t, expected, coalesced).Coalesced)
	require.Equal(t, []string{changed.String(), coalesced.String()}, expected.byAttribute["host.name=node-2"])
	require.EqualValues(t, 1_000, expected.metadataFrom)
	require.Equal(t, expected, lookupNative(t, restarted, attributes...))

	// One more change in the windows open at the restart, on both engines.
	more := []step{
		// Over the limits of changed's metadata and identity, which their windows already took a change of.
		{5_000, changed, "h3", "d", "node-1"},
		// Into the coalesced metadata run and the coalesced version of a, which absorb it.
		{5_000, coalesced, "h4", "a", "node-4"},
		// A new identity, over the limit of the window that already has three.
		{5_000, newcomer, "h1", "c", "node-1"},
	}
	for _, e := range []*Engine{restarted, unstopped} {
		createSeries(t, e, 4_000, newcomer)
		apply(e, more...)
	}
	expected = lookupNative(t, unstopped, attributes...)
	require.True(t, expected.metadata[changed.String()].Coalesced)
	require.True(t, lastResourceVersion(t, expected, changed).Coalesced)
	require.Equal(t, "h4", expected.metadata[coalesced.String()].Versions[2].Metadata.Help)
	require.Equal(t, "node-4", lastResourceVersion(t, expected, coalesced).Descriptive[1].StringValue)
	require.Equal(t, ResourceVersion{EffectiveFrom: 5_000, Coalesced: true}, lastResourceVersion(t, expected, newcomer), "refused")
	require.Equal(t, expected, lookupNative(t, restarted, attributes...))
}

// A crash after a checkpoint taken while ingesting restores the checkpoint, which a replay from it completes: the
// engine then answers every lookup as the one that took the stream without stopping.
//
// The stream goes on while the checkpoint writes the shard's series, a batch at a time: it interns new values and
// changes descriptions, which the series written after it refer to, and the engine's section, written after the shards,
// must have. The replay goes over observations the checkpoint has, of the series written after them, which must change
// nothing: neither the change limits' counts, nor the new identities'. Out of order, within the window, the replayed
// samples of those series are taken again, and so are their observations.
//
// Nothing the limits refuse is replayed: a replay decides again on what they refused, which the engine's section, from
// the end of the checkpoint, may then have room for (see nativeSnapshot.writeEngine).
func TestOpenEngine_RestoresNativeMetadataAfterCheckpointWhileIngesting(t *testing.T) {
	// More series than a snapshot writes in a batch, in one shard, so that the checkpoint pauses between its batches of
	// the series, which a duty below 1 makes long.
	require.Greater(t, checkpointSeries, snapshotBatch)
	series := make([]promlabels.Labels, checkpointSeries)
	for n := range series {
		series[n] = nativeSeriesLabels(fmt.Sprint(n))
	}
	// The series the stream changes: the first, written before the checkpoint's first pause, and the last, after its last.
	changing := []promlabels.Labels{series[0], series[len(series)/2], series[len(series)-1]}
	// Each step a new metadata value and description, and every fourth a new identity: in windows of 1s, ten steps, the
	// change limits let through some of each, and the new identities fill the windows that have three, from step 20 on
	// every other one: a replay that counted one of them again would refuse the next.
	// Between the windows' ends: a series' observation of another identity right at the end of its coalesced run's
	// window leaves the run that goes on from there empty, which a replay of it merges away.
	step := func(e *Engine, k int) error {
		return appendNative(e, int64(10_050+k*100), counterMetadata(fmt.Sprint("v", k)), testResourceAttributes(fmt.Sprint("service-", k/4), fmt.Sprint("node-", k)), changing...)
	}
	open := func(dir string) *Engine {
		e, err := OpenEngine(dir, "tenant", EngineOptions{Shards: 1, OutOfOrderTimeWindowMs: 3_600_000, Checkpoints: true, SecondaryHashFunction: secondaryHash, IndexedResourceAttributes: []string{"host.name"}})
		require.NoError(t, err)
		e.SetNativeMetricMetadata(true)
		e.SetNativeResources(true)
		e.SetMetricMetadataChangeLimit(3, 1_000)
		e.SetResourceLimits(ResourceLimits{WindowMs: 1_000, MaxIdentityChangesPerSeries: 1, MaxVersionsPerIdentity: 2, MaxNewIdentities: 3})
		return e
	}
	dir := t.TempDir()
	e := open(dir)
	createSeries(t, e, 1_000, series...)
	// Enough steps for the stream to go over every limit before the checkpoint, and few enough in all for the series'
	// runs to stay under the cap on them, which would drop the runs from before the checkpoint on both engines alike.
	const before, during = 20, 30
	for k := range before {
		require.NoError(t, step(e, k))
	}

	// The steps before the checkpoint are in the head, as the records up to the Kafka offsets the ingester commits
	// before a checkpoint are: what a restore of it replays from. The steps go on a millisecond apart while it's written.
	var applied atomic.Int64
	applied.Store(before)
	stop := make(chan struct{})
	var ingesting sync.WaitGroup
	ingesting.Go(func() {
		for k := before; k < before+during; k++ {
			select {
			case <-stop:
				return
			case <-time.After(time.Millisecond):
			}
			if !assert.NoError(t, step(e, k)) {
				return
			}
			applied.Store(int64(k + 1))
		}
	})
	// The ingester's duty.
	require.NoError(t, e.Checkpoint(t.Context(), fmt.Append(nil, before), 0.1))
	require.Greater(t, int(applied.Load()), before, "the stream went on while the checkpoint was written")
	close(stop)
	ingesting.Wait()
	total := int(applied.Load())

	crashed := open(dir)
	t.Cleanup(func() { _ = e.Close(); _ = crashed.Close() })
	require.True(t, crashed.Restored())
	require.Equal(t, fmt.Append(nil, before), crashed.RestoredCheckpoint())
	for k := before; k < total; k++ {
		require.NoError(t, step(crashed, k))
	}
	attributes := []mimirpb.ResourceAttribute{{Key: "service.name", StringValue: "service-0"}, {Key: "host.name", StringValue: "node-1"}}
	expected := lookupNative(t, e, attributes...)
	// The stream went over the change limits, whose counts the replay must not change, refused nothing, and kept every
	// run.
	require.True(t, expected.metadata[series[0].String()].Coalesced)
	require.False(t, expected.metadata[series[0].String()].Truncated)
	require.Positive(t, e.ResourceStats().CoalescedVersions)
	require.Positive(t, e.ResourceStats().CoalescedIdentityChanges)
	require.Zero(t, e.ResourceStats().Refused)
	require.Equal(t, expected, lookupNative(t, crashed, attributes...))
}

// nativeSnapshotFixture is the snapshot in the native layout that TestOpenEngine_RestoresNativeSnapshotLayout reads.
var nativeSnapshotFixture = filepath.Join("testdata", "snapshot-native-v8.bin")

// TestWriteNativeSnapshotFixture writes nativeSnapshotFixture when SERIESSTORE_WRITE_NATIVE_SNAPSHOT_FIXTURE is set.
// The file is what the native layout on the plain layout 8 wrote, which later builds must read: write it once, and a
// later layout's to a file of its own.
func TestWriteNativeSnapshotFixture(t *testing.T) {
	if os.Getenv("SERIESSTORE_WRITE_NATIVE_SNAPSHOT_FIXTURE") == "" {
		t.Skip("writes the native snapshot fixture only when SERIESSTORE_WRITE_NATIVE_SNAPSHOT_FIXTURE is set")
	}
	dir := t.TempDir()
	e, err := OpenEngine(dir, "tenant", EngineOptions{Shards: 2, IndexedResourceAttributes: []string{"host.name"}})
	require.NoError(t, err)
	e.SetNativeMetricMetadata(true)
	e.SetNativeResources(true)
	pod0, pod1, plain := nativeSeriesLabels("0"), nativeSeriesLabels("1"), promlabels.FromStrings(model.MetricNameLabel, "up")
	createSeries(t, e, 0, pod0, pod1, plain)
	require.NoError(t, appendNative(e, 1_000, counterMetadata("Requests."), testResourceAttributes("api", "node-1"), pod0, pod1))
	require.NoError(t, appendNative(e, 2_000, counterMetadata("Requests served."), testResourceAttributes("api", "node-2"), pod0))
	require.NoError(t, e.Close())
	written, err := os.ReadFile(filepath.Join(dir, storeDirName, snapshotFileName))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(nativeSnapshotFixture, written, 0o644))
}

// The native layout on the plain layout 8, as TestWriteNativeSnapshotFixture wrote it: two series of an identity
// with native metadata, one of which changed its metadata and the identity's description, and one without any. A
// merge that changes the plain layout's code must keep reading it, as the reader reads the plain layouts before it.
func TestOpenEngine_RestoresNativeSnapshotLayout(t *testing.T) {
	data, err := os.ReadFile(nativeSnapshotFixture)
	require.NoError(t, err)
	require.Equal(t, "MIMIRHN8", string(data[:8]))
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, storeDirName), 0o755))
	for shard := range 2 {
		require.NoError(t, os.MkdirAll(shardDir(filepath.Join(dir, storeDirName), shard), 0o755))
		require.NoError(t, os.MkdirAll(coldDir(filepath.Join(dir, storeDirName), shard), 0o755))
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, storeDirName, snapshotFileName), data, 0o644))
	opts := EngineOptions{Shards: 2, IndexedResourceAttributes: []string{"host.name"}}
	e, err := OpenEngine(dir, "tenant", opts)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, e.Close()) })
	pod0, pod1 := nativeSeriesLabels("0"), nativeSeriesLabels("1")
	requests := metadata.Metadata{Type: model.MetricTypeCounter, Help: "Requests."}
	served := metadata.Metadata{Type: model.MetricTypeCounter, Help: "Requests served."}
	// The identity's versions are its series', whichever of them they were observed through.
	versions := []ResourceVersion{testResourceVersion(1_000, "api", "node-1"), testResourceVersion(2_000, "api", "node-2")}
	// Round 1 reads what the restored engine wrote back.
	for round := range 2 {
		require.True(t, e.Restored())
		require.Equal(t, uint64(3), e.NumSeries())
		lookups := lookupNative(t, e, mimirpb.ResourceAttribute{Key: "service.name", StringValue: "api"}, mimirpb.ResourceAttribute{Key: "host.name", StringValue: "node-1"})
		require.Equal(t, map[string]NativeMetricMetadataSeries{
			pod0.String(): {Labels: pod0, Versions: []MetricMetadataVersion{{EffectiveFrom: 1_000, Metadata: requests}, {EffectiveFrom: 2_000, Metadata: served}}, NewestSample: 2_000},
			pod1.String(): {Labels: pod1, Versions: []MetricMetadataVersion{{EffectiveFrom: 1_000, Metadata: requests}}, NewestSample: 1_000},
		}, lookups.metadata)
		require.Equal(t, map[string]NativeResourcesSeries{
			pod0.String(): {Labels: pod0, Versions: versions, NewestSample: 2_000},
			pod1.String(): {Labels: pod1, Versions: versions, NewestSample: 1_000},
		}, lookups.resources)
		require.Equal(t, lookups.resources, lookups.byTarget)
		require.Equal(t, map[string][]string{"service.name=api": {pod0.String(), pod1.String()}, "host.name=node-1": {pod0.String(), pod1.String()}}, lookups.byAttribute)
		require.EqualValues(t, 1_000, lookups.metadataFrom)
		require.EqualValues(t, 1_000, lookups.resourcesFrom)
		if round == 0 {
			require.NoError(t, e.Close())
			e, err = OpenEngine(dir, "tenant", opts)
			require.NoError(t, err)
		}
	}
}

// A tenant's native switch turned off and on again starts the coverage again at the batch that turns it back on: the
// engine observed nothing in between, which native info() would otherwise serve the versions from before over. Off, it
// covers nothing. The runs from before stay, and age out with the head.
func TestEngine_SetNativeSwitch_OffAndOnAgainStartsTheCoverageAgain(t *testing.T) {
	for name, native := range map[string]struct {
		set           func(e *Engine, enabled bool)
		coverageStart func(e *Engine) int64
		// When the series' versions start.
		versionsFrom func(t *testing.T, e *Engine, series promlabels.Labels) []int64
	}{
		"metric metadata": {
			set:           (*Engine).SetNativeMetricMetadata,
			coverageStart: (*Engine).NativeMetricMetadataCoverageStart,
			versionsFrom: func(t *testing.T, e *Engine, series promlabels.Labels) []int64 {
				var from []int64
				for _, v := range lookupNative(t, e).metadata[series.String()].Versions {
					from = append(from, v.EffectiveFrom)
				}
				return from
			},
		},
		"resources": {
			set:           (*Engine).SetNativeResources,
			coverageStart: (*Engine).NativeResourcesCoverageStart,
			versionsFrom: func(t *testing.T, e *Engine, series promlabels.Labels) []int64 {
				var from []int64
				for _, v := range resourceVersionsOf(t, e, series) {
					from = append(from, v.EffectiveFrom)
				}
				return from
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			e, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 1, SecondaryHashFunction: secondaryHash})
			require.NoError(t, err)
			t.Cleanup(func() { _ = e.Close() })
			l := nativeSeriesLabels("0")
			createSeries(t, e, 0, l)
			batch := func(at int64) {
				require.NoError(t, appendNative(e, at, counterMetadata("Requests."), testResourceAttributes("api", "node-1"), l))
			}
			native.set(e, true)
			batch(1_000)
			require.EqualValues(t, 1_000, native.coverageStart(e))
			native.set(e, false)
			batch(2_000)
			require.Equal(t, int64(math.MaxInt64), native.coverageStart(e))
			native.set(e, true)
			batch(3_000)
			require.EqualValues(t, 3_000, native.coverageStart(e))
			require.Equal(t, []int64{1_000}, native.versionsFrom(t, e, l))
		})
	}
}
