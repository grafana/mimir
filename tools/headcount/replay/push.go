// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/golang/snappy"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/tools/headcount/model"
)

// buildRequest returns a remote-write request holding one float sample
// (timestamp t, value v) for every series in batch, and its snappy-compressed
// protobuf encoding, which is what Mimir's /api/v1/push expects.
func buildRequest(series []model.Series, batch []int, t int64, v float64) (*mimirpb.WriteRequest, []byte, error) {
	req := &mimirpb.WriteRequest{Timeseries: make([]mimirpb.PreallocTimeseries, 0, len(batch))}
	for _, i := range batch {
		req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
			Labels:  mimirpb.FromLabelsToLabelAdapters(series[i].Labels),
			Samples: []mimirpb.Sample{{TimestampMs: t, Value: v}},
		}})
	}
	raw, err := req.Marshal()
	if err != nil {
		return nil, nil, fmt.Errorf("marshalling write request: %w", err)
	}
	return req, snappy.Encode(nil, raw), nil
}

// pusher sends encoded write requests to one Mimir tenant.
type pusher struct {
	url    string // Full push URL, e.g. http://localhost:8080/api/v1/push.
	tenant string
	client *http.Client
	// retries is how many times a 429 or 5xx response is retried, with
	// backoff, before the request is reported as failed.
	retries int
}

// push posts body and returns nil on 2xx. A 400 is final: Mimir uses it
// for samples it will never accept (too old, out of order, over limits),
// so retrying cannot help and the caller must know. 429 and 5xx are
// retried.
func (p *pusher) push(ctx context.Context, body []byte) error {
	backoff := 100 * time.Millisecond
	for attempt := 0; ; attempt++ {
		status, msg, err := p.post(ctx, body)
		switch {
		case err == nil && status/100 == 2:
			return nil
		case err == nil && status != http.StatusTooManyRequests && status/100 != 5:
			return fmt.Errorf("push rejected with HTTP %d: %s", status, msg)
		case attempt >= p.retries:
			if err != nil {
				return fmt.Errorf("push failed after %d attempts: %w", attempt+1, err)
			}
			return fmt.Errorf("push failed after %d attempts with HTTP %d: %s", attempt+1, status, msg)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, 5*time.Second)
	}
}

func (p *pusher) post(ctx context.Context, body []byte) (int, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url, bytes.NewReader(body))
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("Content-Encoding", "snappy")
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	req.Header.Set("X-Scope-OrgID", p.tenant)

	resp, err := p.client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return resp.StatusCode, string(bytes.TrimSpace(msg)), nil
}
