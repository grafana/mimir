package ingest

import (
	"encoding/binary"
	"fmt"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/twmb/franz-go/pkg/kgo"
)

type RecordVersion uint32

const (
	// Record Version 0 is a valid remote write 1.0 request with no provided version header.
	RecordVersionZero RecordVersion = iota
	// Record Version 1 is a valid remote write 1.0 request with a record version header present.
	// The value is identical to record version 0. It can be understood by a consumer speaking record version 0.
	RecordVersionOne

	RecordVersionHeaderKey = "X-Record-Version"
)

func validateRecordVersion(version int) error {
	v := RecordVersion(version)
	switch v {
	case RecordVersionZero:
		return nil
	case RecordVersionOne:
		return nil
	default:
		return fmt.Errorf("unknown record version %d", version)
	}
}

func recordVersionHeader(version RecordVersion) kgo.RecordHeader {
	return kgo.RecordHeader{
		Key:   RecordVersionHeaderKey,
		Value: recordVersionToHeaderValue(version),
	}
}

func recordVersionToHeaderValue(version RecordVersion) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(version))
	return b[:]
}

func headerValueToRecordVersion(value []byte) RecordVersion {
	return RecordVersion(binary.BigEndian.Uint32(value))
}

func recordSerializerFromCfg(cfg KafkaConfig) recordSerializer {
	version := RecordVersion(cfg.ProducerSupportedRecordVersion)
	switch version {
	case RecordVersionZero:
		return versionZeroRecordSerializer{}
	case RecordVersionOne:
		return versionOneRecordSerializer{}
	default:
		return versionZeroRecordSerializer{}
	}
}

// recordSerializer converts a WriteRequest to one or more kafka records.
type recordSerializer interface {
	ToRecords(partitionID int32, tenantID string, req *mimirpb.WriteRequest, maxSize int) ([]*kgo.Record, error)
}

type versionZeroRecordSerializer struct{}

func (v versionZeroRecordSerializer) ToRecords(partitionID int32, tenantID string, req *mimirpb.WriteRequest, maxSize int) ([]*kgo.Record, error) {
	return marshalWriteRequestToRecords(partitionID, tenantID, req, maxSize)
}

type versionOneRecordSerializer struct{}

func (v versionOneRecordSerializer) ToRecords(partitionID int32, tenantID string, req *mimirpb.WriteRequest, maxSize int) ([]*kgo.Record, error) {
	records, err := marshalWriteRequestToRecords(partitionID, tenantID, req, maxSize)
	if err != nil {
		return nil, err
	}
	for _, r := range records {
		r.Headers = append(r.Headers, recordVersionHeader(RecordVersionOne))
	}
	return records, nil
}
