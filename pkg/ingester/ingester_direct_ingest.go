// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

func (i *Ingester) IngestRecord(ctx context.Context, content []byte, version int) error {
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
