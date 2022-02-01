// SPDX-License-Identifier: AGPL-3.0-only

package push

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/prometheusremotewrite"
	"github.com/prometheus/prometheus/prompb"
	"github.com/weaveworks/common/middleware"
	"go.opentelemetry.io/collector/model/otlpgrpc"
	"go.opentelemetry.io/collector/model/pdata"

	"github.com/grafana/mimir/pkg/mimirpb"
)

const (
	pbContentType   = "application/x-protobuf"
	jsonContentType = "application/json"
)

func HandlerForOTLP(
	maxRecvMsgSize int,
	sourceIPs *middleware.SourceIPExtractor,
	allowSkipLabelNameValidation bool,
	push Func,
) http.Handler {
	return handler(maxRecvMsgSize, sourceIPs, allowSkipLabelNameValidation, push, func(ctx context.Context, r *http.Request, maxSize int, dst []byte, req *mimirpb.PreallocWriteRequest) ([]byte, error) {
		var decoderFunc func(buf []byte) (otlpgrpc.MetricsRequest, error)
		contentType := r.Header.Get("Content-Type")
		switch contentType {
		case pbContentType:
			decoderFunc = otlpgrpc.UnmarshalMetricsRequest

		case jsonContentType:
			decoderFunc = otlpgrpc.UnmarshalJSONMetricsRequest
		}

		reader := io.LimitReader(r.Body, int64(maxRecvMsgSize)+1)
		body, err := io.ReadAll(reader)
		if err != nil {
			return body, err
		}
		if err = r.Body.Close(); err != nil {
			return body, err
		}

		otlpReq, err := decoderFunc(body)
		if err != nil {
			return body, err
		}

		metrics, err := otelMetricsToTimeseries(ctx, otlpReq.Metrics())
		if err != nil {
			return body, err
		}

		req.Timeseries = metrics
		return body, nil
	})
}

func otelMetricsToTimeseries(ctx context.Context, md pdata.Metrics) ([]mimirpb.PreallocTimeseries, error) {
	tsMap, _, errs := prometheusremotewrite.MetricsToPRW("", nil, md)
	if errs != nil {
		// TODO: Handle parse errors.
		// Also, the ignored returned argument is the number of samples that were dropped. We should
		// record them somehow.
		fmt.Println(errs)
	}

	mimirTs := make([]mimirpb.PreallocTimeseries, 0, len(tsMap))
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

	return mimirpb.PreallocTimeseries{
		TimeSeries: &mimirpb.TimeSeries{
			Labels:    labels,
			Samples:   samples,
			Exemplars: exemplars,
		},
	}
}
