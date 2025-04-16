package ingest

import (
	"encoding/binary"
	"fmt"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/twmb/franz-go/pkg/kgo"
)

const RecordVersionHeaderKey = "X-Record-Version"

func validateRecordVersion(version int) error {
	switch version {
	case 0:
		return nil
	case 1:
		return nil
	default:
		return fmt.Errorf("unknown record version %d", version)
	}
}

func recordVersionHeader(version int) kgo.RecordHeader {
	return kgo.RecordHeader{
		Key:   RecordVersionHeaderKey,
		Value: recordVersionToHeaderValue(version),
	}
}

func recordVersionToHeaderValue(version int) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(version))
	return b[:]
}

func headerValueToRecordVersion(value []byte) int {
	return int(binary.BigEndian.Uint32(value))
}

func recordSerializerFromCfg(cfg KafkaConfig) recordSerializer {
	switch cfg.ProducerSupportedRecordVersion {
	case 0:
		return versionZeroRecordSerializer{}
	case 1:
		return versionOneRecordSerializer{}
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
	return marshalWriteRequestToRecords(partitionID, tenantID, req, maxSize)
}

// versionOneRecordSerializer produces records of version 1.
// Record Version 1 is a valid remote write 1.0 request with a record version header present.
// The value is identical to record version 0. It can be understood by a consumer speaking record version 0.
type versionOneRecordSerializer struct{}

func (v versionOneRecordSerializer) ToRecords(partitionID int32, tenantID string, req *mimirpb.WriteRequest, maxSize int) ([]*kgo.Record, error) {
	records, err := marshalWriteRequestToRecords(partitionID, tenantID, req, maxSize)
	if err != nil {
		return nil, err
	}
	for _, r := range records {
		r.Headers = append(r.Headers, recordVersionHeader(1))
	}
	return records, nil
}
