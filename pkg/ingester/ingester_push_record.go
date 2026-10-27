// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/grafana/dskit/tenant"
	"go.uber.org/atomic"

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
	// The decoding of the whole record, or of the series of subset only.
	decode *mimirpb.PreallocWriteRequest
	subset []int
	// The content of the record of the subset's series, which its decoding refers to.
	subsetContent *[]byte
	// The series of the record built from it, by index, for what needs them without them being decoded.
	built map[int]*mimirpb.PreallocTimeseries
	// Why the record couldn't be decoded, which leaves it without series.
	decodeErr error
}

// emptyPushSeries is the series of a record that couldn't be decoded.
var emptyPushSeries = mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{}}

// decoded is the whole record decoded.
func (p *pushRequest) decoded() *mimirpb.WriteRequest {
	if p.req == nil {
		p.releaseDecode()
		p.decode, p.subset = &mimirpb.PreallocWriteRequest{}, nil
		p.decodeErr = ingest.DeserializeRecordContent(p.content, p.decode, p.version)
		p.req = &p.decode.WriteRequest
		recordStats.whole.Add(1)
	}
	return p.req
}

// decodedSubset decodes the series of the record at indices, which are in order, into a slice of all of them.
func (p *pushRequest) decodedSubset(indices []int) []mimirpb.PreallocTimeseries {
	all := make([]mimirpb.PreallocTimeseries, p.record.Len())
	if p.req != nil {
		copy(all, p.req.Timeseries)
		return all
	}
	p.releaseDecode()
	p.decode, p.subset = &mimirpb.PreallocWriteRequest{}, indices
	p.subsetContent = subsetContents.Get().(*[]byte)
	*p.subsetContent = p.record.Subset(indices, *p.subsetContent)
	p.decodeErr = ingest.DeserializeRecordContent(*p.subsetContent, p.decode, p.version)
	recordStats.subsets.Add(1)
	for position, index := range indices {
		if position < len(p.decode.Timeseries) {
			all[index] = p.decode.Timeseries[position]
		} else {
			all[index] = emptyPushSeries
		}
	}
	return all
}

// at is the series at index: decoded, or built from the record.
func (p *pushRequest) at(index int) *mimirpb.PreallocTimeseries {
	if p.record == nil {
		if req := p.decoded(); index < len(req.Timeseries) {
			return &req.Timeseries[index]
		}
		return &emptyPushSeries
	}
	if p.req != nil {
		if index < len(p.req.Timeseries) {
			return &p.req.Timeseries[index]
		}
		return &emptyPushSeries
	}
	if position, ok := slices.BinarySearch(p.subset, index); ok && position < len(p.decode.Timeseries) {
		return &p.decode.Timeseries[position]
	}
	if series, ok := p.built[index]; ok {
		return series
	}
	if p.built == nil {
		p.built = map[int]*mimirpb.PreallocTimeseries{}
	}
	lbls, samples := p.record.Series(index)
	series := &mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{Labels: lbls, Samples: samples}}
	p.built[index] = series
	return series
}

// series is the decoded series, none for a record whose series the engine ingests.
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

func (p *pushRequest) metadata() []*mimirpb.MetricMetadata {
	if p.req != nil {
		return p.req.GetMetadata()
	}
	return p.record.Metadata()
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

func (p *pushRequest) releaseDecode() {
	if p.decode != nil {
		p.decode.FreeBuffer()
		mimirpb.ReuseSlice(p.decode.Timeseries)
		p.decode = nil
	}
	if p.subsetContent != nil {
		subsetContents.Put(p.subsetContent)
		p.subsetContent = nil
	}
}

// subsetContents are the buffers of the records of subsets of series, which decoding them refers to until the push
// is done.
var subsetContents = sync.Pool{New: func() any { return new([]byte) }}

func (p *pushRequest) release() {
	if p.record != nil {
		p.record.Release()
		p.record = nil
	}
	p.releaseDecode()
}

// recordStats counts how records are ingested. Temporary.
var recordStats struct {
	records, direct, whole, subsets atomic.Uint64
}

// AcceptsRecords implements ingest.RecordPusher: records are pushed as they are when the ingester's engines read them,
// and else decoded by the reader, which spreads their series over its goroutines.
func (i *Ingester) AcceptsRecords() bool {
	return i.cfg.IngestStorageConfig.Enabled && i.enginesReadRecords()
}

// PushRecord implements ingest.RecordPusher.
func (i *Ingester) PushRecord(ctx context.Context, content []byte, version int) error {
	// What decoding the record would instrument, which reading it in place doesn't go through.
	content, releaseContent := mimirpb.InstrumentRefLeaks(content)
	defer releaseContent()
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
	recordStats.records.Add(1)
	if req.record != nil {
		recordStats.direct.Add(1)
	}
	if req.record == nil || !req.record.Complete() {
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
