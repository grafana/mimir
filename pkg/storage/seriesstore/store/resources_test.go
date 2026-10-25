// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/prometheus/common/model"
	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

func testResourceAttributes(service, host string) []mimirpb.ResourceAttribute {
	return []mimirpb.ResourceAttribute{
		{Key: "service.name", StringValue: service},
		{Key: "host.name", StringValue: host},
		{Key: "service.instance.id", StringValue: "1"},
		{Key: "host.cores", Type: mimirpb.RESOURCE_ATTRIBUTE_INT, IntValue: 8},
	}
}

// testResourceVersion is the version of testResourceAttributes from a time on.
func testResourceVersion(from int64, service, host string) ResourceVersion {
	return ResourceVersion{
		EffectiveFrom: from,
		Identifying:   []mimirpb.ResourceAttribute{{Key: "service.instance.id", StringValue: "1"}, {Key: "service.name", StringValue: service}},
		Descriptive:   []mimirpb.ResourceAttribute{{Key: "host.cores", Type: mimirpb.RESOURCE_ATTRIBUTE_INT, IntValue: 8}, {Key: "host.name", StringValue: host}},
	}
}

func resourceRecord(t *testing.T, at int64, attributes []mimirpb.ResourceAttribute, lbls ...promlabels.Labels) *RW2Record {
	req := &mimirpb.WriteRequest{ResourceTable: []mimirpb.ResourceEntry{{Attributes: attributes}}}
	for _, l := range lbls {
		req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
			Labels:      mimirpb.FromLabelsToLabelAdapters(l),
			Samples:     []mimirpb.Sample{{TimestampMs: at, Value: 1}},
			ResourceRef: 1,
		}})
	}
	records, _, err := ingest.RecordSerializerFromVersion(2).ToRecords("ingest", 0, "tenant", req, 64<<20)
	require.NoError(t, err)
	require.Len(t, records, 1)
	record, err := ReadRW2(records[0].Value, ingest.V2CommonSymbols.GetSlice(), ingest.V2RecordSymbolOffset)
	require.NoError(t, err)
	t.Cleanup(record.Release)
	return record
}

// resourceVersionsOf returns the resource versions of the series with lbls.
func resourceVersionsOf(t *testing.T, e *Engine, lbls promlabels.Labels) []ResourceVersion {
	var matchers []*promlabels.Matcher
	lbls.Range(func(l promlabels.Label) {
		matchers = append(matchers, promlabels.MustNewMatcher(promlabels.MatchEqual, l.Name, l.Value))
	})
	series, _, err := e.NativeResources(context.Background(), [][]*promlabels.Matcher{matchers}, math.MinInt64, math.MaxInt64, 0)
	require.NoError(t, err)
	if len(series) == 0 {
		return nil
	}
	require.Len(t, series, 1)
	return series[0].Versions
}

func asResourceContext(attributes []mimirpb.ResourceAttribute) *storage.ResourceContext {
	rc := &storage.ResourceContext{}
	for _, a := range attributes {
		rc.Attributes = append(rc.Attributes, storage.ResourceAttribute{Key: a.Key, Type: storage.ResourceAttributeType(a.Type), String: a.StringValue, Bool: a.BoolValue, Int: a.IntValue, Double: a.DoubleValue, Bytes: a.BytesValue})
	}
	return rc
}

// The canonical encoding sorts the attributes of a kind by key, keeps their types, and decodes back.
func TestEncodeResourceAttributes(t *testing.T) {
	attributes := []mimirpb.ResourceAttribute{
		{Key: "zone", StringValue: "a"},
		{Key: "service.name", StringValue: "frontend"},
		{Key: "debug", Type: mimirpb.RESOURCE_ATTRIBUTE_BOOL, BoolValue: true},
		{Key: "cores", Type: mimirpb.RESOURCE_ATTRIBUTE_INT, IntValue: -8},
		{Key: "ratio", Type: mimirpb.RESOURCE_ATTRIBUTE_DOUBLE, DoubleValue: 0.5},
		{Key: "id", Type: mimirpb.RESOURCE_ATTRIBUTE_BYTES, BytesValue: []byte{1, 2}},
	}
	identifying, _ := encodeResourceAttributes(nil, attributes, true, nil)
	descriptive, _ := encodeResourceAttributes(nil, attributes, false, nil)
	require.Equal(t, []mimirpb.ResourceAttribute{{Key: "service.name", StringValue: "frontend"}}, decodeResourceAttributes(string(identifying), nil))
	require.Equal(t, []mimirpb.ResourceAttribute{attributes[3], attributes[2], attributes[5], attributes[4], attributes[0]}, decodeResourceAttributes(string(descriptive), nil))

	reversed := make([]mimirpb.ResourceAttribute, 0, len(attributes))
	for i := len(attributes) - 1; i >= 0; i-- {
		reversed = append(reversed, attributes[i])
	}
	again, _ := encodeResourceAttributes(nil, reversed, false, nil)
	require.Equal(t, descriptive, again)
	asString, _ := encodeResourceAttributes(nil, []mimirpb.ResourceAttribute{{Key: "cores", StringValue: "-8"}}, false, nil)
	asInt, _ := encodeResourceAttributes(nil, []mimirpb.ResourceAttribute{{Key: "cores", Type: mimirpb.RESOURCE_ATTRIBUTE_INT, IntValue: -8}}, false, nil)
	require.NotEqual(t, asString, asInt, "a value's type is part of it")
}

func TestEngineKeepsSeriesResourcesOnEveryPath(t *testing.T) {
	e, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 4, SecondaryHashFunction: secondaryHash})
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.Close() })
	e.SetNativeResources(true)
	lbls := promlabels.FromStrings(model.MetricNameLabel, "requests_total", "pod", "pod-0")
	other := promlabels.FromStrings(model.MetricNameLabel, "requests_total", "pod", "pod-1")

	// The appender creates the series, and keeps its resource.
	app := e.AppenderV2(context.Background())
	ref, err := app.Append(0, lbls, 0, 1_000, 1, nil, nil, storage.AppendV2Options{Resource: asResourceContext(testResourceAttributes("frontend", "node-1"))})
	require.NoError(t, err)
	_, err = app.Append(0, other, 0, 1_000, 1, nil, nil, storage.AppendV2Options{})
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	require.NotZero(t, ref)
	require.Equal(t, []ResourceVersion{testResourceVersion(1_000, "frontend", "node-1")}, resourceVersionsOf(t, e, lbls))

	// A record with the same resource is the unchanged path for the first series, and the memo's hit for the second.
	hits := e.resources.stats.memoHits.Load()
	_, ingested, err := e.AppendRW2(resourceRecord(t, 2_000, testResourceAttributes("frontend", "node-1"), lbls, other), 0, 1<<62, 0, false, &rw2Sink{})
	require.NoError(t, err)
	require.Equal(t, 2, ingested)
	require.Equal(t, hits+1, e.resources.stats.memoHits.Load())
	require.Equal(t, []ResourceVersion{testResourceVersion(1_000, "frontend", "node-1")}, resourceVersionsOf(t, e, lbls))
	require.Equal(t, []ResourceVersion{testResourceVersion(2_000, "frontend", "node-1")}, resourceVersionsOf(t, e, other))

	// A record with another host is a version of the identity, which both series have; a decoded batch with another
	// service changes the first series' identity.
	_, _, err = e.AppendRW2(resourceRecord(t, 3_000, testResourceAttributes("frontend", "node-2"), other), 0, 1<<62, 0, false, &rw2Sink{})
	require.NoError(t, err)
	backend := &mimirpb.ResourceEntry{Attributes: testResourceAttributes("backend", "node-3")}
	batch := []mimirpb.PreallocTimeseries{{TimeSeries: &mimirpb.TimeSeries{Labels: mimirpb.FromLabelsToLabelAdapters(lbls), Samples: []mimirpb.Sample{{TimestampMs: 4_000, Value: 1}}, Resource: backend}}}
	_, _, err = e.AppendFloats(batch, []int{0}, 0, 1<<62, 0, false, &rw2Sink{})
	require.NoError(t, err)
	require.Equal(t, []ResourceVersion{
		testResourceVersion(1_000, "frontend", "node-1"),
		testResourceVersion(3_000, "frontend", "node-2"),
		testResourceVersion(4_000, "backend", "node-3"),
	}, resourceVersionsOf(t, e, lbls))
	require.Equal(t, []ResourceVersion{testResourceVersion(2_000, "frontend", "node-1"), testResourceVersion(3_000, "frontend", "node-2")}, resourceVersionsOf(t, e, other))
	stats := e.ResourceStats()
	require.EqualValues(t, 2, stats.Identities)
	require.EqualValues(t, 3, stats.Descriptions)
	require.EqualValues(t, 2, stats.Series)
	require.EqualValues(t, 3, stats.Runs)

	// Quiet zeros and staleness markers carry no resource, and nothing is kept with the switch off.
	app = e.AppenderV2(context.Background())
	_, err = app.Append(ref, lbls, 0, 5_000, math.Float64frombits(value.QuietZeroNaN), nil, nil, storage.AppendV2Options{Resource: asResourceContext(testResourceAttributes("quiet", "node-4"))})
	require.NoError(t, err)
	_, err = app.Append(ref, lbls, 0, 6_000, math.Float64frombits(value.StaleNaN), nil, nil, storage.AppendV2Options{Resource: asResourceContext(testResourceAttributes("stale", "node-4"))})
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	e.SetNativeResources(false)
	_, _, err = e.AppendRW2(resourceRecord(t, 7_000, testResourceAttributes("off", "node-4"), lbls), 0, 1<<62, 0, false, &rw2Sink{})
	require.NoError(t, err)
	require.Len(t, resourceVersionsOf(t, e, lbls), 3)
	require.EqualValues(t, 2, e.ResourceStats().Identities)

	// A time range picks the versions in force during it.
	series, _, err := e.NativeResources(context.Background(), [][]*promlabels.Matcher{{promlabels.MustNewMatcher(promlabels.MatchEqual, "pod", "pod-0")}}, 3_500, 3_600, 0)
	require.NoError(t, err)
	require.Equal(t, []ResourceVersion{testResourceVersion(3_000, "frontend", "node-2")}, series[0].Versions)
}

// observeResource observes the attributes for the series at t, as a batch would, with a memo of its own.
func observeResource(r *resources, m *shardResources, series *Series, attributes []mimirpb.ResourceAttribute, t int64) {
	memo := &resourceMemo{r: r}
	defer memo.release()
	o := memo.byResourceEntry(&mimirpb.ResourceEntry{Attributes: attributes}, t)
	id := o.identity
	if id == 0 {
		id = resourceUnknown
	} else {
		r.observeIdentity(o.identity, o.description, t, o.isNew)
	}
	m.observe(series, id, t, 0)
}

// runs returns the series' runs as each identity's service name, with "?" for an unknown one, and a "*" on a
// coalesced one.
func serviceRuns(r *resources, m *shardResources, series *Series) []string {
	var out []string
	for _, run := range m.runsOf(series, nil) {
		name := "?"
		for _, a := range decodeResourceAttributes(r.identities.value(run.id), nil) {
			if a.Key == "service.name" {
				name = a.StringValue
			}
		}
		if run.id&resourceUnknown != 0 {
			name += "*"
		}
		out = append(out, name)
	}
	return out
}

// A series' identity changes past the limit make its resource unknown, with the latest identity, until the window
// ends: then the latest identity goes on as a run of its own.
func TestResourceIdentityChangeLimit(t *testing.T) {
	r := newResources()
	r.limits.Store(&ResourceLimits{WindowMs: 10_000, MaxIdentityChangesPerSeries: 1})
	m := newShardResources(r)
	series := &Series{ref: 1}
	for _, o := range []struct {
		at      int64
		service string
	}{{1_000, "a"}, {2_000, "b"}, {3_000, "c"}, {4_000, "d"}, {12_000, "e"}} {
		observeResource(r, m, series, testResourceAttributes(o.service, "node"), o.at)
	}
	require.Equal(t, []string{"a", "b", "d*", "d", "e"}, serviceRuns(r, m, series))
	var froms []int64
	for _, run := range m.runsOf(series, nil) {
		froms = append(froms, run.from)
	}
	require.Equal(t, []int64{1_000, 2_000, 3_000, 10_000, 12_000}, froms)
	require.EqualValues(t, 1, r.stats.coalescedIdentityChanges.Load())

	// An out-of-order observation goes among the older runs, unless a coalesced run covers it.
	observeResource(r, m, series, testResourceAttributes("x", "node"), 1_500)
	observeResource(r, m, series, testResourceAttributes("y", "node"), 5_000)
	require.Equal(t, []string{"a", "x", "b", "d*", "d", "e"}, serviceRuns(r, m, series))
}

// An identity's description changes past the limit coalesce into one version, with the latest description, until
// the window ends.
func TestResourceVersionLimit(t *testing.T) {
	r := newResources()
	r.limits.Store(&ResourceLimits{WindowMs: 10_000, MaxVersionsPerIdentity: 1})
	m := newShardResources(r)
	series := &Series{ref: 1}
	for _, o := range []struct {
		at   int64
		host string
	}{{1_000, "a"}, {2_000, "b"}, {3_000, "c"}, {4_000, "d"}, {12_000, "e"}} {
		observeResource(r, m, series, testResourceAttributes("frontend", o.host), o.at)
	}
	e := &Engine{resources: r}
	var hosts []string
	var coalesced []bool
	for _, v := range e.resourceVersionsLocked(m, series, math.MinInt64, math.MaxInt64, nil) {
		hosts = append(hosts, v.Descriptive[1].StringValue)
		coalesced = append(coalesced, v.Coalesced)
	}
	require.Equal(t, []string{"a", "b", "d", "d", "e"}, hosts)
	require.Equal(t, []bool{false, false, true, false, false}, coalesced)
	require.EqualValues(t, 1, r.stats.coalescedVersions.Load())
}

// New identities past the limit, and values past the limit on bytes, are refused: the series' resource is unknown.
func TestResourceNewIdentityAndBytesLimits(t *testing.T) {
	r := newResources()
	r.limits.Store(&ResourceLimits{WindowMs: 10_000, MaxNewIdentities: 1})
	m := newShardResources(r)
	first, second := &Series{ref: 1}, &Series{ref: 2}
	observeResource(r, m, first, testResourceAttributes("a", "node"), 1_000)
	observeResource(r, m, second, testResourceAttributes("b", "node"), 2_000)
	observeResource(r, m, first, testResourceAttributes("a", "node"), 3_000)
	require.Equal(t, []string{"a"}, serviceRuns(r, m, first))
	require.Equal(t, []string{"?*"}, serviceRuns(r, m, second))
	// The next window takes a new identity again.
	observeResource(r, m, second, testResourceAttributes("b", "node"), 11_000)
	require.Equal(t, []string{"?*", "b"}, serviceRuns(r, m, second))

	r.limits.Store(&ResourceLimits{MaxBytes: r.bytes.Load()})
	observeResource(r, m, first, testResourceAttributes("c", "node"), 12_000)
	require.Equal(t, []string{"a", "?*"}, serviceRuns(r, m, first))
	require.EqualValues(t, 2, r.stats.refused.Load())
}

// Every way a series' runs go gives up their references, and the compaction's trim reclaims what nothing refers to,
// out of the reverse index too.
func TestResourcesAreReleased(t *testing.T) {
	r := newResources()
	indexed := map[string]struct{}{"host.name": {}}
	r.indexed.Store(&indexed)
	m := newShardResources(r)
	first, second := &Series{ref: 1}, &Series{ref: 2}
	observeResource(r, m, first, testResourceAttributes("a", "node-1"), 1_000)
	observeResource(r, m, first, testResourceAttributes("b", "node-2"), 2_000)
	observeResource(r, m, second, testResourceAttributes("b", "node-2"), 2_000)

	seriesOf := func(key, value string) []uint64 {
		return r.seriesWithAttribute(mimirpb.ResourceAttribute{Key: key, StringValue: value}).ToArray()
	}
	require.Equal(t, []uint64{1}, seriesOf("service.name", "a"))
	require.Equal(t, []uint64{1, 2}, seriesOf("service.name", "b"))
	require.Equal(t, []uint64{1, 2}, seriesOf("host.name", "node-2"))
	require.Empty(t, seriesOf("service.instance.id", "2"))

	// Trimming the first series' older run leaves its identity to nothing, which the compaction reclaims.
	m.trim(first, 2_000)
	r.trim(2_000)
	require.Empty(t, seriesOf("service.name", "a"))
	require.EqualValues(t, 1, r.identities.values.Load())
	require.EqualValues(t, 1, r.descriptions.values.Load())

	// Dropping both series leaves nothing.
	m.drop(first)
	m.drop(second)
	r.trim(2_000)
	require.Zero(t, r.identities.values.Load())
	require.Zero(t, r.descriptions.values.Load())
	require.Zero(t, r.bytes.Load())
	require.Empty(t, r.state.byAttribute)
	require.Empty(t, r.state.versions)
	require.Zero(t, m.series)
	require.Zero(t, m.runs)
}

// Ingesting, looking up, evicting series and reclaiming values at once keep every reference count right.
func TestResourcesUnderConcurrentEvictionsAndLookups(t *testing.T) {
	e, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 4, SecondaryHashFunction: secondaryHash})
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.Close() })
	e.SetNativeResources(true)
	const series = 200
	var lbls []promlabels.Labels
	var refs []storage.SeriesRef
	app := e.AppenderV2(context.Background())
	for n := range series {
		l := promlabels.FromStrings(model.MetricNameLabel, "requests_total", "pod", fmt.Sprintf("pod-%03d", n))
		ref, err := app.Append(0, l, 0, 1_000, 1, nil, nil, storage.AppendV2Options{Resource: asResourceContext(testResourceAttributes("s0", "h0"))})
		require.NoError(t, err)
		lbls, refs = append(lbls, l), append(refs, ref)
	}
	require.NoError(t, app.Commit())

	var wg sync.WaitGroup
	done := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(done)
		for step := range 100 {
			attributes := testResourceAttributes(fmt.Sprintf("s%d", step%2), fmt.Sprintf("h%d", step%3))
			_, _, err := e.AppendRW2(resourceRecord(t, int64(2_000+step*1_000), attributes, lbls...), 0, 1<<62, 0, false, &rw2Sink{})
			assert.NoError(t, err)
		}
	}()
	background := func(work func(n int)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; ; n++ {
				select {
				case <-done:
					return
				default:
				}
				work(n)
			}
		}()
	}
	matchers := [][]*promlabels.Matcher{{promlabels.MustNewMatcher(promlabels.MatchEqual, model.MetricNameLabel, "requests_total")}}
	background(func(int) {
		found, _, err := e.NativeResources(context.Background(), matchers, math.MinInt64, math.MaxInt64, 10)
		assert.NoError(t, err)
		for _, s := range found {
			for _, v := range s.Versions {
				assert.Len(t, v.Identifying, 2)
				assert.Len(t, v.Descriptive, 2)
			}
		}
	})
	background(func(n int) {
		assert.NoError(t, e.CompactSelectedSeries(refs[n%series:n%series+1]))
	})
	background(func(n int) { e.resources.trim(int64(n)) })
	wg.Wait()

	require.NoError(t, e.CompactSelectedSeries(refs))
	e.resources.trim(math.MaxInt64)
	require.Zero(t, e.resources.identities.values.Load())
	require.Zero(t, e.resources.descriptions.values.Load())
	for _, d := range []*resourceDictionary{e.resources.identities, e.resources.descriptions} {
		for _, page := range *d.pages.Load() {
			for index := range page {
				require.GreaterOrEqual(t, page[index].refs.Load(), int64(0))
			}
		}
	}
}
