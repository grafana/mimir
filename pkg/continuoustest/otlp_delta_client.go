// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"context"
	"maps"
	"strings"
	"time"

	"github.com/prometheus/common/model"

	"github.com/grafana/mimir/pkg/streamingpromql/compat"
)

const useStartTimestampsHeader = "X-Prometheus-Use-Start-Timestamps"

type otlpDeltaClient struct {
	MimirClient
}

func (c *otlpDeltaClient) QueryRange(ctx context.Context, query string, start, end time.Time, step time.Duration, options ...RequestOption) (model.Matrix, error) {
	return c.MimirClient.QueryRange(ctx, query, start, end, step, deltaQueryOptions(options)...)
}

func (c *otlpDeltaClient) Query(ctx context.Context, query string, ts time.Time, options ...RequestOption) (model.Vector, error) {
	return c.MimirClient.Query(ctx, query, ts, deltaQueryOptions(options)...)
}

func deltaQueryOptions(options []RequestOption) []RequestOption {
	return append(options, func(options *requestOptions) {
		// Copy caller-owned headers before adding the delta query settings.
		headers := maps.Clone(options.additionalHeaders)
		if headers == nil {
			headers = make(map[string]string)
		}

		for name := range headers {
			if strings.EqualFold(name, useStartTimestampsHeader) || strings.EqualFold(name, compat.ForceFallbackHeaderName) || strings.EqualFold(name, "Cache-Control") {
				delete(headers, name)
			}
		}

		headers[useStartTimestampsHeader] = "true"
		headers[compat.ForceFallbackHeaderName] = "true"

		// Cached responses do not distinguish start-timestamp query semantics.
		WithResultsCacheEnabled(false)(options)
		WithAdditionalHeaders(headers)(options)
	})
}
