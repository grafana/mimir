// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"

	"github.com/grafana/dskit/tenant"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util/validation"
)

func (i *Ingester) IngestRecord(ctx context.Context, content []byte, version int) error {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return mapPushErrorToErrorWithStatus(err)
	}

	// Only existing tenants can read records directly. The decoded path creates the
	// tenant after checking admission, and avoids opening storage for metadata-only writes.
	if db := i.getTSDB(userID); db != nil {
		if engine, ok := db.engine().(directIngestEngine); ok {
			if record := engine.ReadRecord(content, version); record != nil {
				// Keep the record bound to the engine that parsed it even if idle cleanup
				// races this write. The append lock rejects writes to the closing engine.
				req := pushRequest{record: record, db: db, recordSize: int64(len(content))}
				if err := i.pushWithCleanup(ctx, req, record.Release); err != nil {
					return mapPushErrorToErrorWithStatus(err)
				}
				return nil
			}
		}
	}

	return i.pushDecodedRecord(ctx, content, version)
}

func (i *Ingester) pushDecodedRecord(ctx context.Context, content []byte, version int) error {
	var req mimirpb.PreallocWriteRequest
	if err := ingest.DeserializeRecordContent(content, &req, version); err != nil {
		req.FreeBuffer()
		mimirpb.ReuseSlice(req.Timeseries)
		return fmt.Errorf("%w: %w", ingest.ErrMalformedRecord, err)
	}
	return i.PushToStorageAndReleaseRequest(ctx, &req.WriteRequest)
}

// pushRequest exposes the fields the ingester needs before and after writing the samples.
type pushRequest struct {
	decoded    *mimirpb.WriteRequest
	record     directIngestRecord
	db         *userTSDB
	recordSize int64
}

func (r pushRequest) size() int64 {
	if r.record != nil {
		return r.recordSize
	}
	return int64(r.decoded.Size())
}

func (r pushRequest) numSeries() int {
	if r.record != nil {
		return r.record.NumSeries()
	}
	return len(r.decoded.Timeseries)
}

func (r pushRequest) metadata() []*mimirpb.MetricMetadata {
	if r.record != nil {
		return r.record.Metadata()
	}
	return r.decoded.GetMetadata()
}

func (r pushRequest) minTimestamp() int64 {
	if r.record != nil {
		return r.record.MinTimestamp()
	}
	return r.decoded.MinTimestamp()
}

func (r pushRequest) source() mimirpb.WriteRequest_SourceEnum {
	if r.record != nil {
		return r.record.Source()
	}
	return r.decoded.Source
}

func (r pushRequest) groupLabel(limits *validation.Overrides, userID string) string {
	if r.record != nil {
		if groupLabel := limits.SeparateMetricsGroupLabel(userID); groupLabel != "" {
			return validation.GroupLabelFromLabels(groupLabel, r.record.FirstSeriesLabels())
		}
		return ""
	}
	return validation.GroupLabel(limits, userID, r.decoded.Timeseries)
}

func (r pushRequest) ingest(ctx *ingestContext, engine tenantEngine) error {
	if r.record != nil {
		return r.record.Ingest(ctx)
	}
	return engine.Ingest(ctx, r.decoded.Timeseries)
}
