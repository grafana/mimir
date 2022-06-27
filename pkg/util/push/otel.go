// SPDX-License-Identifier: AGPL-3.0-only

package push

import (
	"context"
	"fmt"
	"io"
	"net/http"

	kitlog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/prometheusremotewrite"
	"github.com/prometheus/prometheus/prompb"
	"github.com/weaveworks/common/middleware"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	pbContentType   = "application/x-protobuf"
	jsonContentType = "application/json"

	otelParseError = "otlp_parse_error"
	maxErrMsgLen   = 1024

	messageSizeLargerErrFmt = "received message larger than max (%d vs %d)"
)

func HandlerForOTLP(
	maxRecvMsgSize int,
	sourceIPs *middleware.SourceIPExtractor,
	allowSkipLabelNameValidation bool,
	push Func,
) http.Handler {
	return handler(maxRecvMsgSize, sourceIPs, allowSkipLabelNameValidation, push, func(ctx context.Context, r *http.Request, maxSize int, dst []byte, req *mimirpb.PreallocWriteRequest) ([]byte, error) {
		var decoderFunc func(buf []byte) (pmetricotlp.Request, error)

		logger := log.WithContext(ctx, log.Logger)

		contentType := r.Header.Get("Content-Type")
		switch contentType {
		case pbContentType:
			decoderFunc = func(buf []byte) (pmetricotlp.Request, error) {
				req := pmetricotlp.NewRequest()
				return req, req.UnmarshalProto(buf)
			}

		case jsonContentType:
			decoderFunc = func(buf []byte) (pmetricotlp.Request, error) {
				req := pmetricotlp.NewRequest()
				return req, req.UnmarshalJSON(buf)
			}

		default:
			return nil, fmt.Errorf("unsupported content type: %s, supported: [%s, %s]", contentType, jsonContentType, pbContentType)
		}

		if r.ContentLength > int64(maxSize) {
			return nil, fmt.Errorf(messageSizeLargerErrFmt, r.ContentLength, maxSize)
		}

		reader := http.MaxBytesReader(nil, r.Body, int64(maxRecvMsgSize))
		body, err := io.ReadAll(reader)
		if err != nil {
			r.Body.Close()
			return body, err
		}

		if err = r.Body.Close(); err != nil {
			return body, err
		}

		otlpReq, err := decoderFunc(body)
		if err != nil {
			return body, err
		}

		metrics, err := otelMetricsToTimeseries(ctx, logger, otlpReq.Metrics())
		if err != nil {
			return body, err
		}

		req.Timeseries = metrics
		return body, nil
	})
}

func otelMetricsToTimeseries(ctx context.Context, logger kitlog.Logger, md pmetric.Metrics) ([]mimirpb.PreallocTimeseries, error) {
	tsMap, errs := prometheusremotewrite.FromMetrics(md, prometheusremotewrite.Settings{})

	dropped := md.MetricCount() - len(tsMap)

	if errs != nil {
		userID, err := tenant.TenantID(ctx)
		if err != nil {
			return nil, err
		}
		validation.DiscardedSamples.WithLabelValues(otelParseError, userID).Add(float64(dropped))

		parseErrs := errs.Error()
		if len(parseErrs) > maxErrMsgLen {
			parseErrs = parseErrs[:maxErrMsgLen]
		}

		level.Warn(logger).Log("msg", "OTLP parse error", "err", parseErrs)
	}

	mimirTs := mimirpb.PreallocTimeseriesSliceFromPool()
	for _, promTs := range tsMap {
		mimirTs = append(mimirTs, promToMimirTimeseries(promTs))
	}

	return mimirTs, nil
}

func promToMimirTimeseries(promTs *prompb.TimeSeries) mimirpb.PreallocTimeseries {
	labels := make([]mimirpb.LabelAdapter, 0, len(promTs.Labels))
	for _, label := range promTs.Labels {
		labels = append(labels, mimirpb.LabelAdapter{
			Name:  label.Name,
			Value: label.Value,
		})
	}

	samples := make([]mimirpb.Sample, 0, len(promTs.Samples))
	for _, sample := range promTs.Samples {
		samples = append(samples, mimirpb.Sample{
			TimestampMs: sample.Timestamp,
			Value:       sample.Value,
		})
	}

	exemplars := make([]mimirpb.Exemplar, 0, len(promTs.Exemplars))
	for _, exemplar := range promTs.Exemplars {
		labels := make([]mimirpb.LabelAdapter, 0, len(exemplar.Labels))
		for _, label := range promTs.Labels {
			labels = append(labels, mimirpb.LabelAdapter{
				Name:  label.Name,
				Value: label.Value,
			})
		}

		exemplars = append(exemplars, mimirpb.Exemplar{
			Labels:      labels,
			Value:       exemplar.Value,
			TimestampMs: exemplar.Timestamp,
		})
	}

	ts := mimirpb.TimeseriesFromPool()
	ts.Labels = labels
	ts.Samples = samples
	ts.Exemplars = exemplars

	return mimirpb.PreallocTimeseries{TimeSeries: ts}
}
