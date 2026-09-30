// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ComparisonResult is the result of the comparison of the primary and secondary responses of one call.
type ComparisonResult string

const (
	ComparisonMatch    ComparisonResult = "match"
	ComparisonMismatch ComparisonResult = "mismatch"
	ComparisonSkipped  ComparisonResult = "skipped"
)

// ResponseComparator compares the responses of one backend type.
type ResponseComparator interface {
	// Compare compares the responses of one successful call to the primary and the secondary backend.
	// If the result is ComparisonMismatch or ComparisonSkipped, the error describes the reason.
	Compare(fullMethod string, primary, secondary []proxiedMessage) (ComparisonResult, error)
}

// compareCall compares the primary and secondary calls of call. call.Secondary must not be nil.
// The rules for the call status come first, and the comparator compares only the responses of successful calls.
func compareCall(comparator ResponseComparator, call teeCall) (ComparisonResult, error) {
	primary, secondary := call.Primary, *call.Secondary

	if primary.Aborted {
		return ComparisonSkipped, errors.New("the primary call is incomplete because the client side failed")
	}
	if secondary.Aborted {
		return ComparisonSkipped, errors.New("the secondary call was aborted because the secondary backend was too slow to receive the requests")
	}

	primaryCode, secondaryCode := status.Code(primary.Err), status.Code(secondary.Err)
	if primaryCode != secondaryCode {
		return ComparisonMismatch, fmt.Errorf("the primary backend returned status %s but the secondary backend returned status %s", primaryCode, secondaryCode)
	}
	if primaryCode != codes.OK {
		// Both calls failed with the same status code.
		return ComparisonMatch, nil
	}

	if err := errors.Join(call.RequestDecodeErr, primary.DecodeErr, secondary.DecodeErr); err != nil {
		return ComparisonSkipped, fmt.Errorf("failed to decode the messages of the call: %w", err)
	}

	return comparator.Compare(call.FullMethod, primary.Responses, secondary.Responses)
}

// opaqueComparator compares the raw bytes of the responses.
type opaqueComparator struct{}

func (opaqueComparator) Compare(_ string, primary, secondary []proxiedMessage) (ComparisonResult, error) {
	if len(primary) != len(secondary) {
		return ComparisonMismatch, fmt.Errorf("the primary backend returned %d response messages but the secondary backend returned %d", len(primary), len(secondary))
	}
	for i := range primary {
		if !bytes.Equal(primary[i].Payload, secondary[i].Payload) {
			return ComparisonMismatch, fmt.Errorf("response message %d is different", i)
		}
	}
	return ComparisonMatch, nil
}

// teeMetrics are the metrics of the tee handler.
type teeMetrics struct {
	requestDuration     *prometheus.HistogramVec
	timeToFirstResponse *prometheus.HistogramVec
	responsesCompared   *prometheus.CounterVec
	relativeDuration    *prometheus.HistogramVec
}

func newTeeMetrics(reg prometheus.Registerer) *teeMetrics {
	const namespace = "cortex_grpctee"
	durationBuckets := prometheus.ExponentialBuckets(0.001, 4, 9)

	return &teeMetrics{
		requestDuration: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "backend_request_duration_seconds",
			Help:      "Time from when grpc-tee opened the backend stream to the final status of the backend call.",
			Buckets:   durationBuckets,
		}, []string{"backend", "method", "status_code"}),
		timeToFirstResponse: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "backend_time_to_first_response_seconds",
			Help:      "Time from when grpc-tee opened the backend stream to the first response message of the backend.",
			Buckets:   durationBuckets,
		}, []string{"backend", "method"}),
		responsesCompared: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "responses_compared_total",
			Help:      "Total number of calls for which grpc-tee compared the primary and secondary responses, by result.",
		}, []string{"method", "result"}),
		// The primary duration includes the time that the client takes to read the responses, so this metric
		// is biased towards a faster secondary. Refer to backendCall.Duration.
		relativeDuration: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Namespace:                       namespace,
			Name:                            "backend_response_relative_duration_seconds",
			Help:                            "Duration of the secondary backend call minus the duration of the primary backend call.",
			NativeHistogramBucketFactor:     2,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: time.Hour,
		}, []string{"method"}),
	}
}

func (m *teeMetrics) observeBackendCall(method string, call backendCall) {
	m.requestDuration.WithLabelValues(call.Backend, method, status.Code(call.Err).String()).Observe(call.Duration.Seconds())
	if len(call.Responses) > 0 {
		m.timeToFirstResponse.WithLabelValues(call.Backend, method).Observe(call.TimeToFirstResponse.Seconds())
	}
}

// maxLoggedMessageLength is the maximum length of a decoded message in the logs.
const maxLoggedMessageLength = 1024

// newCallFinisher returns the function that the tee handler calls when a call is complete.
// The function updates the metrics, compares the responses if there is a secondary call, and logs the call.
func newCallFinisher(comparator ResponseComparator, metrics *teeMetrics, logger log.Logger) func(teeCall) {
	return func(call teeCall) {
		keyvals := []any{
			"msg", "proxied call",
			"method", call.FullMethod,
			"requests", len(call.Requests),
			"request_bytes", call.RequestBytes,
		}
		if len(call.Requests) > 0 && call.Requests[0].Decoded != nil {
			// Most calls have one request message, so the log shows only the first one.
			keyvals = append(keyvals, "first_request", truncate(call.Requests[0].Decoded.String(), maxLoggedMessageLength))
		}

		metrics.observeBackendCall(call.FullMethod, call.Primary)
		keyvals = appendBackendCall(keyvals, "primary", call.Primary)

		logLevel := level.Info
		if call.Secondary != nil {
			metrics.observeBackendCall(call.FullMethod, *call.Secondary)
			metrics.relativeDuration.WithLabelValues(call.FullMethod).Observe((call.Secondary.Duration - call.Primary.Duration).Seconds())
			keyvals = appendBackendCall(keyvals, "secondary", *call.Secondary)

			result, err := compareCall(comparator, call)
			metrics.responsesCompared.WithLabelValues(call.FullMethod, string(result)).Inc()
			keyvals = append(keyvals, "result", result)
			if err != nil {
				keyvals = append(keyvals, "reason", err)
			}
			if result != ComparisonMatch {
				logLevel = level.Warn
			}
		}

		logLevel(logger).Log(keyvals...)
	}
}

func appendBackendCall(keyvals []any, role string, call backendCall) []any {
	keyvals = append(keyvals,
		role+"_backend", call.Backend,
		role+"_responses", len(call.Responses),
		role+"_response_bytes", call.ResponseBytes,
		role+"_duration", call.Duration,
		role+"_time_to_first_response", call.TimeToFirstResponse,
		role+"_status", status.Code(call.Err),
	)
	if call.Err != nil {
		keyvals = append(keyvals, role+"_err", call.Err)
	}
	if call.Aborted {
		keyvals = append(keyvals, role+"_aborted", true)
	}
	if call.DecodeErr != nil {
		keyvals = append(keyvals, role+"_decode_err", call.DecodeErr)
	}
	return keyvals
}

func truncate(s string, maxLength int) string {
	if len(s) <= maxLength {
		return s
	}
	return s[:maxLength] + "..."
}
