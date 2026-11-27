// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"math"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

func TestIngester_IngestRecord(t *testing.T) {
	i := prepareHealthyIngester(t, nil)
	pusher := NewIngesterProfilingWrapper(i)

	t.Run("decoded fallback creates tenant", func(t *testing.T) {
		const tenant = "decoded"
		ctx := user.InjectOrgID(t.Context(), tenant)
		require.Nil(t, i.getTSDB(tenant))

		req := mockWriteRequest(t, labels.FromStrings("__name__", "fallback"), 1, time.Now().UnixMilli())
		defer mimirpb.ReuseSlice(req.Timeseries)
		content, err := req.Marshal()
		require.NoError(t, err)
		require.NoError(t, pusher.IngestRecord(ctx, content, 1))
		require.EqualValues(t, 1, i.getTSDB(tenant).Head().NumSeries())
	})

	t.Run("malformed record", func(t *testing.T) {
		const tenant = "malformed"
		ctx := user.InjectOrgID(t.Context(), tenant)
		require.ErrorIs(t, pusher.IngestRecord(ctx, []byte{0xff}, 1), ingest.ErrMalformedRecord)
		require.Nil(t, i.getTSDB(tenant))
	})

	t.Run("direct ingest and fallback for an existing tenant", func(t *testing.T) {
		const tenant = "direct"
		ctx := user.InjectOrgID(t.Context(), tenant)
		req := mockWriteRequest(t, labels.FromStrings("__name__", "direct"), 1, time.Now().UnixMilli())
		defer mimirpb.ReuseSlice(req.Timeseries)

		db, err := i.createTSDB(tenant, 0, true)
		require.NoError(t, err)
		record := &testDirectIngestRecord{decoded: req, engine: db.engine()}
		db.db = testDirectIngestEngine{tenantEngine: db.engine(), record: record}

		i.tsdbsMtx.Lock()
		i.tsdbs[tenant] = db
		i.tsdbsMtx.Unlock()

		// This content is only valid for the fake engine, so decoding would fail.
		require.NoError(t, pusher.IngestRecord(ctx, []byte("direct"), 2))
		require.Equal(t, 1, record.ingests)
		require.Equal(t, 1, record.releases)
		require.EqualValues(t, 1, db.Head().NumSeries())

		fallback := mockWriteRequest(t, labels.FromStrings("__name__", "fallback"), 1, time.Now().UnixMilli())
		defer mimirpb.ReuseSlice(fallback.Timeseries)
		content, err := fallback.Marshal()
		require.NoError(t, err)
		require.NoError(t, pusher.IngestRecord(ctx, content, 1))
		require.EqualValues(t, 2, db.Head().NumSeries())

		db.stateMtx.Lock()
		db.state = forceCompacting
		db.forcedCompactionMaxTime = math.MaxInt64
		db.stateMtx.Unlock()
		defer func() {
			db.stateMtx.Lock()
			db.state = active
			db.stateMtx.Unlock()
		}()

		require.ErrorContains(t, pusher.IngestRecord(ctx, []byte("direct"), 2), errTSDBForcedCompaction.Error())
		require.Equal(t, 1, record.ingests)
		require.Equal(t, 2, record.releases)
	})
}

type testDirectIngestEngine struct {
	tenantEngine
	record *testDirectIngestRecord
}

func (e testDirectIngestEngine) ReadRecord(content []byte, version int) directIngestRecord {
	if version == 2 && string(content) == "direct" {
		return e.record
	}
	return nil
}

type testDirectIngestRecord struct {
	decoded *mimirpb.WriteRequest
	engine  tenantEngine

	ingests, releases int
}

func (r *testDirectIngestRecord) Metadata() []*mimirpb.MetricMetadata { return r.decoded.Metadata }
func (r *testDirectIngestRecord) NumSeries() int                      { return len(r.decoded.Timeseries) }
func (r *testDirectIngestRecord) MinTimestamp() int64                 { return r.decoded.MinTimestamp() }
func (r *testDirectIngestRecord) Source() mimirpb.WriteRequest_SourceEnum {
	return r.decoded.Source
}

func (r *testDirectIngestRecord) FirstSeriesLabels() []mimirpb.LabelAdapter {
	return r.decoded.Timeseries[0].Labels
}

func (r *testDirectIngestRecord) Ingest(ctx *ingestContext) error {
	r.ingests++
	return r.engine.Ingest(ctx, r.decoded.Timeseries)
}

func (r *testDirectIngestRecord) Release() { r.releases++ }
