// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"fmt"
	"unsafe"

	"github.com/prometheus/prometheus/model/histogram"

	apierror "github.com/grafana/mimir/pkg/api/error"
)

const QueryResponseMimeType = QueryResponseMimeTypeType + "/" + QueryResponseMimeTypeSubType
const QueryResponseMimeTypeType = "application"
const QueryResponseMimeTypeSubType = "vnd.mimir.queryresponse+protobuf"

func (s QueryStatus) ToPrometheusString() (string, error) {
	switch s {
	case QUERY_STATUS_SUCCESS:
		return "success", nil
	case QUERY_STATUS_ERROR:
		return "error", nil
	default:
		return "", fmt.Errorf("unknown QueryErrorType value: %v (%v)", int32(s), s.String())
	}
}

func StatusFromPrometheusString(s string) (QueryStatus, error) {
	switch s {
	case "success":
		return QUERY_STATUS_SUCCESS, nil
	case "error":
		return QUERY_STATUS_ERROR, nil
	default:
		return QUERY_STATUS_ERROR, fmt.Errorf("unknown Prometheus status value: '%v'", s)
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
	case QUERY_ERROR_TYPE_TOO_LARGE_ENTRY:
		return "too_large_entry", nil
	case QUERY_ERROR_TYPE_TOO_MANY_REQUESTS:
		return "too_many_requests", nil
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
	case "too_large_entry":
		return QUERY_ERROR_TYPE_TOO_LARGE_ENTRY, nil
	case "too_many_requests":
		return QUERY_ERROR_TYPE_TOO_MANY_REQUESTS, nil
	default:
		return QUERY_ERROR_TYPE_NONE, fmt.Errorf("unknown Prometheus error type value: '%v'", s)
	}
}

func ErrorTypeFromAPIErrorType(t apierror.Type) (QueryErrorType, error) {
	switch t {
	case apierror.TypeNone:
		return QUERY_ERROR_TYPE_NONE, nil
	case apierror.TypeTimeout:
		return QUERY_ERROR_TYPE_TIMEOUT, nil
	case apierror.TypeCanceled:
		return QUERY_ERROR_TYPE_CANCELED, nil
	case apierror.TypeExec:
		return QUERY_ERROR_TYPE_EXECUTION, nil
	case apierror.TypeBadData:
		return QUERY_ERROR_TYPE_BAD_DATA, nil
	case apierror.TypeInternal:
		return QUERY_ERROR_TYPE_INTERNAL, nil
	case apierror.TypeUnavailable:
		return QUERY_ERROR_TYPE_UNAVAILABLE, nil
	case apierror.TypeNotFound:
		return QUERY_ERROR_TYPE_NOT_FOUND, nil
	case apierror.TypeTooManyRequests:
		return QUERY_ERROR_TYPE_TOO_MANY_REQUESTS, nil
	case apierror.TypeTooLargeEntry:
		return QUERY_ERROR_TYPE_TOO_LARGE_ENTRY, nil
	case apierror.TypeNotAcceptable:
		return QUERY_ERROR_TYPE_NOT_ACCEPTABLE, nil
	default:
		return QUERY_ERROR_TYPE_NONE, fmt.Errorf("unknown API error type %q", t)
	}
}

func (h *FloatHistogram) ToPrometheusModel() *histogram.FloatHistogram {
	return (*histogram.FloatHistogram)(unsafe.Pointer(h))
}

func FloatHistogramFromPrometheusModel(h *histogram.FloatHistogram) *FloatHistogram {
	return (*FloatHistogram)(unsafe.Pointer(h))
}
