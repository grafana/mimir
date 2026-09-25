// SPDX-License-Identifier: AGPL-3.0-only

// freshness-loadgen writes identical synthetic series to several tenants, so a tenant with a
// delayed_series policy can be compared against a standard one under the same load.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/golang/snappy"

	"github.com/grafana/mimir/pkg/mimirpb"
)

type config struct {
	url       string
	tenants   string
	clusters  int
	pods      int
	handlers  int
	interval  time.Duration
	batchSize int
}

func main() {
	var cfg config
	flag.StringVar(&cfg.url, "url", "http://localhost:8000/api/v1/push", "Remote write URL.")
	flag.StringVar(&cfg.tenants, "tenants", "standard,delayed", "Comma-separated tenants that receive identical series.")
	flag.IntVar(&cfg.clusters, "clusters", 40, "Number of clusters.")
	flag.IntVar(&cfg.pods, "pods", 25, "Pods per cluster.")
	flag.IntVar(&cfg.handlers, "handlers", 10, "HTTP handlers per pod.")
	flag.DurationVar(&cfg.interval, "interval", 15*time.Second, "Write interval.")
	flag.IntVar(&cfg.batchSize, "batch-size", 5000, "Series per remote write request.")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	series := generateSeries(cfg)
	tenants := strings.Split(cfg.tenants, ",")
	log.Printf("writing %d series to each of %v every %s", len(series), tenants, cfg.interval)

	client := &http.Client{Timeout: 30 * time.Second}
	ticker := time.NewTicker(cfg.interval)
	defer ticker.Stop()

	for iteration := 0; ; iteration++ {
		start := time.Now()
		ts := start.UnixMilli()
		for _, tenant := range tenants {
			if err := write(ctx, client, cfg, tenant, series, ts, float64(iteration)); err != nil {
				log.Printf("tenant %s: %v", tenant, err)
			}
		}
		log.Printf("iteration %d written in %s", iteration, time.Since(start).Round(time.Millisecond))

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// generateSeries returns label sets shaped like hosted-Grafana metrics: an HTTP latency histogram
// plus a request counter, per cluster, pod and handler.
func generateSeries(cfg config) [][]mimirpb.LabelAdapter {
	buckets := []string{"0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10", "+Inf"}
	var out [][]mimirpb.LabelAdapter

	for c := 0; c < cfg.clusters; c++ {
		cluster := fmt.Sprintf("prod-%02d", c)
		for p := 0; p < cfg.pods; p++ {
			pod := fmt.Sprintf("grafana-%d", p)
			for h := 0; h < cfg.handlers; h++ {
				handler := fmt.Sprintf("/api/handler-%d", h)
				base := func(name string, extra ...string) []mimirpb.LabelAdapter {
					lbls := []mimirpb.LabelAdapter{{Name: "__name__", Value: name}, {Name: "cluster", Value: cluster}, {Name: "handler", Value: handler}}
					for i := 0; i < len(extra); i += 2 {
						lbls = append(lbls, mimirpb.LabelAdapter{Name: extra[i], Value: extra[i+1]})
					}
					return append(lbls, mimirpb.LabelAdapter{Name: "pod", Value: pod})
				}
				for _, le := range buckets {
					out = append(out, base("grafana_http_request_duration_seconds_bucket", "le", le))
				}
				out = append(out, base("grafana_http_request_duration_seconds_count"), base("grafana_http_request_duration_seconds_sum"))
				out = append(out, base("grafana_api_requests_total", "status", "200"), base("grafana_api_requests_total", "status", "500"))
			}
		}
	}
	return out
}

func write(ctx context.Context, client *http.Client, cfg config, tenant string, series [][]mimirpb.LabelAdapter, ts int64, value float64) error {
	for start := 0; start < len(series); start += cfg.batchSize {
		end := min(start+cfg.batchSize, len(series))
		req := &mimirpb.WriteRequest{Source: mimirpb.API}
		for _, lbls := range series[start:end] {
			req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
				Labels:  lbls,
				Samples: []mimirpb.Sample{{TimestampMs: ts, Value: value}},
			}})
		}
		data, err := req.Marshal()
		if err != nil {
			return err
		}

		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.url, bytes.NewReader(snappy.Encode(nil, data)))
		if err != nil {
			return err
		}
		httpReq.Header.Set("Content-Encoding", "snappy")
		httpReq.Header.Set("Content-Type", "application/x-protobuf")
		httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
		httpReq.Header.Set("X-Scope-OrgID", tenant)

		resp, err := client.Do(httpReq)
		if err != nil {
			return err
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		_ = resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			return fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
	}
	return nil
}
