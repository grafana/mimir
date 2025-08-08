// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"encoding/binary"
	"fmt"

	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/mimirpb"
)

const (
	RecordVersionHeaderKey = "Version"
	LatestRecordVersion    = 2
	V2RecordSymbolOffset   = 64
)

var (
	// Bake in a shared symbols table for extremely common symbols.
	// The index corresponds to the symbol value.
	// The first `V2RecordSymbolOffset` symbols are reserved for this table.
	// Note: V2 is not yet stabilized.
	V2CommonSymbols = mimirpb.NewCommonSymbols([]string{
		// RW2.0 Spec: The first element of the symbols table MUST be an empty string.
		// This ensures that empty/missing refs still map to empty string.
		"",
		// Prometheus/Mimir symbols
		"__name__",
		"__aggregation__",
		"<aggregated>",
		"le",
		"component",
		"cortex_request_duration_seconds_bucket",
		"storage_operation_duration_seconds_bucket",
		// Grafana Labs products
		"grafana",
		"asserts_env",
		"asserts_request_context",
		"asserts_source",
		"asserts_entity_type",
		"asserts_request_type",
		// General symbols
		"name",
		"image",
		// Kubernetes symbols
		"cluster",
		"namespace",
		"pod",
		"job",
		"instance",
		"container",
		"replicaset",
		// Networking, HTTP
		"interface",
		"status_code",
		"resource",
		"operation",
		"method",
		// Common tools
		"kube-system",
		"kube-system/cadvisor",
		"node-exporter",
		"node-exporter/node-exporter",
		"kube-system/kubelet",
		"kube-system/node-local-dns",
		"kube-state-metrics/kube-state-metrics",
		"default/kubernetes",
	})
)

func ValidateRecordVersion(version int) error {
	switch version {
	case 0:
		return nil
	case 1:
		return nil
	case 2:
		return nil
	default:
		return fmt.Errorf("unknown record version %d", version)
	}
}

func RecordVersionHeader(version int) kgo.RecordHeader {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(version))
	return kgo.RecordHeader{
		Key:   RecordVersionHeaderKey,
		Value: b[:],
	}
}

func ParseRecordVersion(rec *kgo.Record) int {
	for i := range rec.Headers {
		if rec.Headers[i].Key == RecordVersionHeaderKey {
			return int(binary.BigEndian.Uint32(rec.Headers[i].Value))
		}
	}
	return 0
}

func recordSerializerFromCfg(cfg KafkaConfig) recordSerializer {
	switch cfg.ProducerRecordVersion {
	case 0:
		return versionZeroRecordSerializer{}
	case 1:
		return versionOneRecordSerializer{}
	case 2:
		return versionTwoRecordSerializer{}
	default:
		return versionZeroRecordSerializer{}
	}
}

// recordSerializer converts a WriteRequest to one or more kafka records.
type recordSerializer interface {
	ToRecords(partitionID int32, tenantID string, req *mimirpb.WriteRequest, maxSize int) ([]*kgo.Record, error)
}

// versionZeroRecordSerializer produces records of version 0.
// Record Version 0 is a valid remote write 1.0 request with no provided version header.
type versionZeroRecordSerializer struct{}

func (v versionZeroRecordSerializer) ToRecords(partitionID int32, tenantID string, req *mimirpb.WriteRequest, maxSize int) ([]*kgo.Record, error) {
	return marshalWriteRequestToRecords(partitionID, tenantID, req, maxSize, mimirpb.SplitWriteRequestByMaxMarshalSize)
}

// versionOneRecordSerializer produces records of version 1.
// Record Version 1 is a valid remote write 1.0 request with a record version header present.
// The value is identical to record version 0. It can be understood by a consumer speaking record version 0.
type versionOneRecordSerializer struct{}

func (v versionOneRecordSerializer) ToRecords(partitionID int32, tenantID string, req *mimirpb.WriteRequest, maxSize int) ([]*kgo.Record, error) {
	records, err := marshalWriteRequestToRecords(partitionID, tenantID, req, maxSize, mimirpb.SplitWriteRequestByMaxMarshalSize)
	if err != nil {
		return nil, err
	}
	for _, r := range records {
		r.Headers = append(r.Headers, RecordVersionHeader(1))
	}
	return records, nil
}

type versionTwoRecordSerializer struct{}

func (v versionTwoRecordSerializer) ToRecords(partitionID int32, tenantID string, req *mimirpb.WriteRequest, maxSize int) ([]*kgo.Record, error) {
	reqv2, err := mimirpb.FromWriteRequestToRW2Request(req, V2CommonSymbols, V2RecordSymbolOffset)
	defer mimirpb.ReuseRW2(reqv2)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert RW1 request to RW2")
	}

	records, err := marshalWriteRequestToRecords(partitionID, tenantID, reqv2, maxSize, splitRequestVersionTwo)
	if err != nil {
		return nil, errors.Wrap(err, "failed to serialise write request")
	}
	for _, r := range records {
		r.Headers = append(r.Headers, RecordVersionHeader(2))
	}
	return records, nil
}

func DeserializeRecordContent(content []byte, wr *mimirpb.PreallocWriteRequest, version int) error {
	switch version {
	case 0:
		// V0 is body-compatible with V1.
		fallthrough
	case 1:
		return deserializeRecordContentV1(content, wr)
	case 2:
		return deserializeRecordContentV2(content, wr)
	default:
		return fmt.Errorf("received a record with an unsupported version: %d, max supported version: %d", version, LatestRecordVersion)
	}
}

func deserializeRecordContentV1(content []byte, wr *mimirpb.PreallocWriteRequest) error {
	return wr.Unmarshal(content)
}

func deserializeRecordContentV2(content []byte, wr *mimirpb.PreallocWriteRequest) error {
	wr.UnmarshalFromRW2 = true
	wr.RW2SymbolOffset = V2RecordSymbolOffset
	wr.RW2CommonSymbols = V2CommonSymbols.GetSlice()
	return wr.Unmarshal(content)
}

// splitRequestVersionTwo adapts mimirpb.SplitWriteRequestByMaxMarshalSizeRW2 to requestSplitter
func splitRequestVersionTwo(wr *mimirpb.WriteRequest, reqSize, maxSize int) []*mimirpb.WriteRequest {
	return mimirpb.SplitWriteRequestByMaxMarshalSizeRW2(wr, reqSize, maxSize, V2RecordSymbolOffset, V2CommonSymbols)
}
