// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"fmt"
	"unsafe"

	"github.com/prometheus/prometheus/model/histogram"
)

const QueryResponseMimeType = QueryResponseMimeTypeType + "/" + QueryResponseMimeTypeSubType
const QueryResponseMimeTypeType = "application"
const QueryResponseMimeTypeSubType = "vnd.mimir.queryresponse+protobuf"

func (s QueryResponse_Status) ToPrometheusString() (string, error) {
	switch s {
	case QueryResponse_SUCCESS:
		return "success", nil
	case QueryResponse_ERROR:
		return "error", nil
	default:
		return "", fmt.Errorf("unknown QueryErrorType value: %v (%v)", int32(s), s.String())
	}
}

func StatusFromPrometheusString(s string) (QueryResponse_Status, error) {
	switch s {
	case "success":
		return QueryResponse_SUCCESS, nil
	case "error":
		return QueryResponse_ERROR, nil
	default:
		return QueryResponse_ERROR, fmt.Errorf("unknown Prometheus status value: '%v'", s)
	}
}

func (t QueryErrorType) ToPrometheusString() (string, error) {
	switch t {
	case QUERY_ERROR_TYPE_NONE:
		return "", nil
	case QUERY_ERROR_TYPE_TIMEOUT:
		return "timeout", nil
	case QUERY_ERROR_TYPE_CANCELED:
		return "canceled", nil
	case QUERY_ERROR_TYPE_EXECUTION:
		return "execution", nil
	case QUERY_ERROR_TYPE_BAD_DATA:
		return "bad_data", nil
	case QUERY_ERROR_TYPE_INTERNAL:
		return "internal", nil
	case QUERY_ERROR_TYPE_UNAVAILABLE:
		return "unavailable", nil
	case QUERY_ERROR_TYPE_NOT_FOUND:
		return "not_found", nil
	case QUERY_ERROR_TYPE_NOT_ACCEPTABLE:
		return "not_acceptable", nil
	default:
		return "", fmt.Errorf("unknown QueryErrorType value: %v (%v)", int32(t), t.String())
	}
}

func ErrorTypeFromPrometheusString(s string) (QueryErrorType, error) {
	switch s {
	case "":
		return QUERY_ERROR_TYPE_NONE, nil
	case "timeout":
		return QUERY_ERROR_TYPE_TIMEOUT, nil
	case "canceled":
		return QUERY_ERROR_TYPE_CANCELED, nil
	case "execution":
		return QUERY_ERROR_TYPE_EXECUTION, nil
	case "bad_data":
		return QUERY_ERROR_TYPE_BAD_DATA, nil
	case "internal":
		return QUERY_ERROR_TYPE_INTERNAL, nil
	case "unavailable":
		return QUERY_ERROR_TYPE_UNAVAILABLE, nil
	case "not_found":
		return QUERY_ERROR_TYPE_NOT_FOUND, nil
	case "not_acceptable":
		return QUERY_ERROR_TYPE_NOT_ACCEPTABLE, nil
	default:
		return QUERY_ERROR_TYPE_NONE, fmt.Errorf("unknown Prometheus error type value: '%v'", s)
	}
}

func (h *FloatHistogram) ToPrometheusModel() *histogram.FloatHistogram {
	return (*histogram.FloatHistogram)(unsafe.Pointer(h))
}

func FloatHistogramFromPrometheusModel(h *histogram.FloatHistogram) *FloatHistogram {
	return (*FloatHistogram)(unsafe.Pointer(h))
}
