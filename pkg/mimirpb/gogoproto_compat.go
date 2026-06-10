// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"fmt"
	"io"
	math_bits "math/bits"
)

// This file preserves parts of the gogoproto-generated API that the wiresmith
// compiler does not emit, so that hand-written code (in this package and in
// consumers) keeps compiling unchanged after the gogoproto -> wiresmith
// migration:
//
//   - varint helpers (sovMimir/encodeVarintMimir/skipMimir) used by the
//     hand-written marshalling code (LabelAdapter, request splitting, ...);
//   - unprefixed enum constant aliases: gogoproto generated them with
//     goproto_enum_prefix=false semantics, while wiresmith always prefixes
//     constants with the enum (or parent message) name like protoc-gen-go.

// Varint helpers copied from the gogoproto-generated mimir.pb.go.

var (
	ErrInvalidLengthMimir        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowMimir          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupMimir = fmt.Errorf("proto: unexpected end of group")
)

func sovMimir(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}

func encodeVarintMimir(dAtA []byte, offset int, v uint64) int {
	offset -= sovMimir(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}

func skipMimir(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowMimir
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowMimir
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
		case 1:
			iNdEx += 8
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowMimir
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthMimir
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupMimir
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthMimir
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

// Unprefixed enum constant aliases matching the gogoproto-generated names.
// Histogram_ResetHint used goproto_enum_prefix=true, so its wiresmith names
// (Histogram_UNKNOWN, ...) already match and need no aliases.

const (
	ERROR_CAUSE_UNKNOWN                ErrorCause = ErrorCause_ERROR_CAUSE_UNKNOWN
	ERROR_CAUSE_REPLICAS_DID_NOT_MATCH ErrorCause = ErrorCause_ERROR_CAUSE_REPLICAS_DID_NOT_MATCH
	ERROR_CAUSE_TOO_MANY_CLUSTERS      ErrorCause = ErrorCause_ERROR_CAUSE_TOO_MANY_CLUSTERS
	ERROR_CAUSE_BAD_DATA               ErrorCause = ErrorCause_ERROR_CAUSE_BAD_DATA
	ERROR_CAUSE_INGESTION_RATE_LIMITED ErrorCause = ErrorCause_ERROR_CAUSE_INGESTION_RATE_LIMITED
	ERROR_CAUSE_REQUEST_RATE_LIMITED   ErrorCause = ErrorCause_ERROR_CAUSE_REQUEST_RATE_LIMITED
	ERROR_CAUSE_INSTANCE_LIMIT         ErrorCause = ErrorCause_ERROR_CAUSE_INSTANCE_LIMIT
	ERROR_CAUSE_SERVICE_UNAVAILABLE    ErrorCause = ErrorCause_ERROR_CAUSE_SERVICE_UNAVAILABLE
	ERROR_CAUSE_TSDB_UNAVAILABLE       ErrorCause = ErrorCause_ERROR_CAUSE_TSDB_UNAVAILABLE
	ERROR_CAUSE_TOO_BUSY               ErrorCause = ErrorCause_ERROR_CAUSE_TOO_BUSY
	ERROR_CAUSE_CIRCUIT_BREAKER_OPEN   ErrorCause = ErrorCause_ERROR_CAUSE_CIRCUIT_BREAKER_OPEN
	ERROR_CAUSE_METHOD_NOT_ALLOWED     ErrorCause = ErrorCause_ERROR_CAUSE_METHOD_NOT_ALLOWED
	ERROR_CAUSE_TENANT_LIMIT           ErrorCause = ErrorCause_ERROR_CAUSE_TENANT_LIMIT
	ERROR_CAUSE_ACTIVE_SERIES_LIMITED  ErrorCause = ErrorCause_ERROR_CAUSE_ACTIVE_SERIES_LIMITED
)

const (
	QUERY_STATUS_ERROR   QueryStatus = QueryStatus_QUERY_STATUS_ERROR
	QUERY_STATUS_SUCCESS QueryStatus = QueryStatus_QUERY_STATUS_SUCCESS
)

const (
	QUERY_ERROR_TYPE_NONE              QueryErrorType = QueryErrorType_QUERY_ERROR_TYPE_NONE
	QUERY_ERROR_TYPE_TIMEOUT           QueryErrorType = QueryErrorType_QUERY_ERROR_TYPE_TIMEOUT
	QUERY_ERROR_TYPE_CANCELED          QueryErrorType = QueryErrorType_QUERY_ERROR_TYPE_CANCELED
	QUERY_ERROR_TYPE_EXECUTION         QueryErrorType = QueryErrorType_QUERY_ERROR_TYPE_EXECUTION
	QUERY_ERROR_TYPE_BAD_DATA          QueryErrorType = QueryErrorType_QUERY_ERROR_TYPE_BAD_DATA
	QUERY_ERROR_TYPE_INTERNAL          QueryErrorType = QueryErrorType_QUERY_ERROR_TYPE_INTERNAL
	QUERY_ERROR_TYPE_UNAVAILABLE       QueryErrorType = QueryErrorType_QUERY_ERROR_TYPE_UNAVAILABLE
	QUERY_ERROR_TYPE_NOT_FOUND         QueryErrorType = QueryErrorType_QUERY_ERROR_TYPE_NOT_FOUND
	QUERY_ERROR_TYPE_NOT_ACCEPTABLE    QueryErrorType = QueryErrorType_QUERY_ERROR_TYPE_NOT_ACCEPTABLE
	QUERY_ERROR_TYPE_TOO_MANY_REQUESTS QueryErrorType = QueryErrorType_QUERY_ERROR_TYPE_TOO_MANY_REQUESTS
	QUERY_ERROR_TYPE_TOO_LARGE_ENTRY   QueryErrorType = QueryErrorType_QUERY_ERROR_TYPE_TOO_LARGE_ENTRY
)

const (
	API  WriteRequest_SourceEnum = WriteRequest_API
	RULE WriteRequest_SourceEnum = WriteRequest_RULE
	OTLP WriteRequest_SourceEnum = WriteRequest_OTLP
)

const (
	UNKNOWN        MetricMetadata_MetricType = MetricMetadata_UNKNOWN
	COUNTER        MetricMetadata_MetricType = MetricMetadata_COUNTER
	GAUGE          MetricMetadata_MetricType = MetricMetadata_GAUGE
	HISTOGRAM      MetricMetadata_MetricType = MetricMetadata_HISTOGRAM
	GAUGEHISTOGRAM MetricMetadata_MetricType = MetricMetadata_GAUGEHISTOGRAM
	SUMMARY        MetricMetadata_MetricType = MetricMetadata_SUMMARY
	INFO           MetricMetadata_MetricType = MetricMetadata_INFO
	STATESET       MetricMetadata_MetricType = MetricMetadata_STATESET
)

const (
	METRIC_TYPE_UNSPECIFIED    MetadataRW2_MetricType = MetadataRW2_METRIC_TYPE_UNSPECIFIED
	METRIC_TYPE_COUNTER        MetadataRW2_MetricType = MetadataRW2_METRIC_TYPE_COUNTER
	METRIC_TYPE_GAUGE          MetadataRW2_MetricType = MetadataRW2_METRIC_TYPE_GAUGE
	METRIC_TYPE_HISTOGRAM      MetadataRW2_MetricType = MetadataRW2_METRIC_TYPE_HISTOGRAM
	METRIC_TYPE_GAUGEHISTOGRAM MetadataRW2_MetricType = MetadataRW2_METRIC_TYPE_GAUGEHISTOGRAM
	METRIC_TYPE_SUMMARY        MetadataRW2_MetricType = MetadataRW2_METRIC_TYPE_SUMMARY
	METRIC_TYPE_INFO           MetadataRW2_MetricType = MetadataRW2_METRIC_TYPE_INFO
	METRIC_TYPE_STATESET       MetadataRW2_MetricType = MetadataRW2_METRIC_TYPE_STATESET
)
