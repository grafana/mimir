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
		return "", fmt.Errorf("unknown QueryResponse_ErrorType value: %v (%v)", int32(s), s.String())
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

func (t QueryResponse_ErrorType) ToPrometheusString() (string, error) {
	switch t {
	case QueryResponse_NONE:
		return "", nil
	case QueryResponse_TIMEOUT:
		return "timeout", nil
	case QueryResponse_CANCELED:
		return "canceled", nil
	case QueryResponse_EXECUTION:
		return "execution", nil
	case QueryResponse_BAD_DATA:
		return "bad_data", nil
	case QueryResponse_INTERNAL:
		return "internal", nil
	case QueryResponse_UNAVAILABLE:
		return "unavailable", nil
	case QueryResponse_NOT_FOUND:
		return "not_found", nil
	case QueryResponse_NOT_ACCEPTABLE:
		return "not_acceptable", nil
	default:
		return "", fmt.Errorf("unknown QueryResponse_ErrorType value: %v (%v)", int32(t), t.String())
	}
}

func ErrorTypeFromPrometheusString(s string) (QueryResponse_ErrorType, error) {
	switch s {
	case "":
		return QueryResponse_NONE, nil
	case "timeout":
		return QueryResponse_TIMEOUT, nil
	case "canceled":
		return QueryResponse_CANCELED, nil
	case "execution":
		return QueryResponse_EXECUTION, nil
	case "bad_data":
		return QueryResponse_BAD_DATA, nil
	case "internal":
		return QueryResponse_INTERNAL, nil
	case "unavailable":
		return QueryResponse_UNAVAILABLE, nil
	case "not_found":
		return QueryResponse_NOT_FOUND, nil
	case "not_acceptable":
		return QueryResponse_NOT_ACCEPTABLE, nil
	default:
		return QueryResponse_NONE, fmt.Errorf("unknown Prometheus error type value: '%v'", s)
	}
}

func (h *FloatHistogram) ToPrometheusModel() *histogram.FloatHistogram {
	return (*histogram.FloatHistogram)(unsafe.Pointer(h))
}

func FloatHistogramFromPrometheusModel(h *histogram.FloatHistogram) *FloatHistogram {
	return (*FloatHistogram)(unsafe.Pointer(h))
}
