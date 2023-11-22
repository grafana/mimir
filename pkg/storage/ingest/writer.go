package ingest

import (
	"context"

	"github.com/grafana/dskit/services"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// Writer is responsible to write incoming data to the ingest storage.
type Writer struct {
	services.Service
}

func NewWriter() *Writer {
	w := &Writer{}
	w.Service = services.NewIdleService(nil, nil)

	return w
}

// WriteSync the input data to the ingest storage. The function blocks until the data has been successfully committed,
// or an error occurred.
func (w *Writer) WriteSync(ctx context.Context, partitionID uint32, userID string, timeseries []mimirpb.PreallocTimeseries, metadata []*mimirpb.MetricMetadata, source mimirpb.WriteRequest_SourceEnum) error {
	return nil
}
