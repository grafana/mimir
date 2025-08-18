package parquet

import (
	arrowpb "github.com/open-telemetry/otel-arrow/go/api/experimental/arrow/v1"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

func Write(batch *arrowpb.BatchArrowRecords) {
	local.NewLocalFileWriter()
	w := writer.NewArrowWriter()
}
