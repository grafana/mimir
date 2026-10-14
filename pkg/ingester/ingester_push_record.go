// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"

	"github.com/grafana/dskit/tenant"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

// pushRequest is what a push ingests: a decoded request, or a record, which an engine that reads records itself
// ingests the series of without decoding it, and which is decoded only for what needs its series.
type pushRequest struct {
	req *mimirpb.WriteRequest

	// For a record.
	content []byte
	version int
	record  ingestRecord
	decode  *mimirpb.PreallocWriteRequest
	// Why the record couldn't be decoded, which leaves it without series.
	decodeErr error
}

// emptyPushSeries is the series of a record that couldn't be decoded.
var emptyPushSeries = mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{}}

func (p *pushRequest) decoded() *mimirpb.WriteRequest {
	if p.req == nil {
		p.decode = &mimirpb.PreallocWriteRequest{}
		p.decodeErr = ingest.DeserializeRecordContent(p.content, p.decode, p.version)
		p.req = &p.decode.WriteRequest
	}
	return p.req
}

// at is the series at index, decoding the record if it is one.
func (p *pushRequest) at(index int) *mimirpb.PreallocTimeseries {
	if req := p.decoded(); index < len(req.Timeseries) {
		return &req.Timeseries[index]
	}
	return &emptyPushSeries
}

// series is the decoded series, none for a record whose floats the engine appends.
func (p *pushRequest) series() []mimirpb.PreallocTimeseries {
	if p.record != nil {
		return nil
	}
	return p.decoded().Timeseries
}

func (p *pushRequest) size() int64 {
	if p.content != nil {
		return int64(len(p.content))
	}
	return int64(p.req.Size())
}

func (p *pushRequest) len() int {
	if p.record != nil {
		return p.record.Len()
	}
	return len(p.decoded().Timeseries)
}

func (p *pushRequest) source() mimirpb.WriteRequest_SourceEnum {
	if p.record != nil {
		return p.record.Source()
	}
	return p.decoded().Source
}

// metadata is the request's metadata: a record with any is decoded before the push.
func (p *pushRequest) metadata() []*mimirpb.MetricMetadata {
	if p.req == nil {
		return nil
	}
	return p.req.GetMetadata()
}

func (p *pushRequest) minTimestamp() int64 {
	if p.record != nil && p.req == nil {
		return p.record.MinTimestamp()
	}
	return p.decoded().MinTimestamp()
}

// withoutRecord is for an engine that ingests decoded series only.
func (p *pushRequest) withoutRecord() {
	if p.record != nil {
		p.decoded()
		p.record.Release()
		p.record = nil
	}
}

func (p *pushRequest) release() {
	if p.record != nil {
		p.record.Release()
		p.record = nil
	}
	if p.decode != nil {
		p.decode.FreeBuffer()
		mimirpb.ReuseSlice(p.decode.Timeseries)
		p.decode = nil
	}
}

// AcceptsRecords implements ingest.RecordPusher: records are pushed as they are, for the engines that read them.
func (i *Ingester) AcceptsRecords() bool {
	return i.cfg.IngestStorageConfig.Enabled && i.cfg.PushRecordsToEngine
}

// PushRecord implements ingest.RecordPusher.
func (i *Ingester) PushRecord(ctx context.Context, content []byte, version int) error {
	req := &pushRequest{content: content, version: version}
	defer req.release()
	// The tenant's engine reads the record, if it does: a tenant without a TSDB yet gets it with its first push.
	if userID, err := tenant.TenantID(ctx); err == nil {
		if db := i.getTSDB(userID); db != nil {
			if reader, ok := db.db.(recordReader); ok {
				req.record, _ = reader.ReadRecord(content, version)
			}
		}
	}
	if req.record == nil || !req.record.OnlyFloats() {
		// What else the record has, or all of it, is read by decoding it, as the reader would have.
		req.decoded()
		if req.decodeErr != nil {
			return fmt.Errorf("%w: %w", ingest.ErrMalformedRecord, req.decodeErr)
		}
	}
	if err := i.push(ctx, req); err != nil {
		return mapPushErrorToErrorWithStatus(err)
	}
	return nil
}
