package arrow

import (
	"fmt"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"

	arrowpb "github.com/open-telemetry/otel-arrow/go/api/experimental/arrow/v1"
	arrowRecord "github.com/open-telemetry/otel-arrow/go/pkg/otel/arrow_record"
)

var (
	producer    arrowRecord.ProducerAPI
	unmarshaler *pmetric.ProtoUnmarshaler
)

func init() {
	producer = arrowRecord.NewProducer()
}

func Marshal(md *metricsv1.MetricsData) ([]byte, error) {
	buf, err := proto.Marshal(md)
	if err != nil {
		return nil, fmt.Errorf("marshal protobuf message: %w", err)
	}

	records, err := unmarshaler.UnmarshalMetrics(buf)
	if err != nil {
		return nil, fmt.Errorf("unmarshal pmetric: %w", err)
	}

	return encode(records)
}

func MarshalBatch(md *metricsv1.MetricsData) (*arrowpb.BatchArrowRecords, error) {
	buf, err := proto.Marshal(md)
	if err != nil {
		return nil, fmt.Errorf("marshal protobuf message: %w", err)
	}

	records, err := unmarshaler.UnmarshalMetrics(buf)
	if err != nil {
		return nil, fmt.Errorf("unmarshal pmetric: %w", err)
	}

	return encodeBatch(records)
}

func encodeBatch(records any) (*arrowpb.BatchArrowRecords, error) {
	var (
		batch *arrowpb.BatchArrowRecords
		err   error
	)

	switch data := records.(type) {
	case ptrace.Traces:
		batch, err = producer.BatchArrowRecordsFromTraces(data)
	case plog.Logs:
		batch, err = producer.BatchArrowRecordsFromLogs(data)
	case pmetric.Metrics:
		batch, err = producer.BatchArrowRecordsFromMetrics(data)
	default:
		return nil, fmt.Errorf("unsupported records type: %T", data)
	}
	if err != nil {
		return nil, fmt.Errorf("batch arrow records: %w", err)
	}

	return batch, nil
}

func encode(records any) ([]byte, error) {
	batch, err := encodeBatch(records)
	if err != nil {
		return nil, err
	}

	buf, err := proto.Marshal(batch)
	if err != nil {
		return nil, fmt.Errorf("marshal arrow batch: %w", err)
	}

	return buf, nil
}
