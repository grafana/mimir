// SPDX-License-Identifier: AGPL-3.0-only

package metrics

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

const sidecarTimeout = 2 * time.Second

// sidecarGatherer gathers the ring sidecar's families over HTTP. Families that describe the
// sidecar's process rather than the ingester's, `process_*` and the ones this process already
// exports like `go_*`, are dropped: both processes' families go out in one response.
type sidecarGatherer struct {
	url    string
	client *http.Client
	local  prometheus.Gatherer
}

func (g sidecarGatherer) Gather() ([]*dto.MetricFamily, error) {
	ctx, cancel := context.WithTimeout(context.Background(), sidecarTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, g.url, nil)
	if err != nil {
		return nil, nil
	}
	// The protobuf format carries native histogram buckets.
	request.Header.Set("Accept", string(expfmt.NewFormat(expfmt.TypeProtoDelim)))
	response, err := g.client.Do(request)
	if err != nil {
		// A missing sidecar leaves the ingester's own families.
		return nil, nil
	}
	defer response.Body.Close()
	families, err := decodeFamilies(response.Body, expfmt.ResponseFormat(response.Header))
	if err != nil {
		return nil, nil
	}
	local := map[string]bool{}
	if g.local != nil {
		if own, err := g.local.Gather(); err == nil {
			for _, family := range own {
				local[family.GetName()] = true
			}
		}
	}
	return sidecarFamilies(families, local), nil
}

func decodeFamilies(body io.Reader, format expfmt.Format) ([]*dto.MetricFamily, error) {
	decoder := expfmt.NewDecoder(body, format)
	var families []*dto.MetricFamily
	for {
		family := &dto.MetricFamily{}
		if err := decoder.Decode(family); err != nil {
			if errors.Is(err, io.EOF) {
				return families, nil
			}
			return families, err
		}
		families = append(families, family)
	}
}

// sidecarFamilies drops the sidecar's `process_*` families and the ones `local` already has.
func sidecarFamilies(families []*dto.MetricFamily, local map[string]bool) []*dto.MetricFamily {
	kept := families[:0]
	for _, family := range families {
		if strings.HasPrefix(family.GetName(), "process_") || local[family.GetName()] {
			continue
		}
		kept = append(kept, family)
	}
	return kept
}

// Handler serves the main registry, with the ring sidecar's metrics appended when sidecarURL is
// set, negotiating the text or protobuf format like Prometheus's own handler.
func Handler(sidecarURL string) http.Handler {
	var gatherer prometheus.Gatherer = Registry
	if sidecarURL != "" {
		gatherer = prometheus.Gatherers{Registry, sidecarGatherer{
			url:    sidecarURL,
			client: &http.Client{Timeout: sidecarTimeout},
			local:  Registry,
		}}
	}
	return promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{ErrorHandling: promhttp.ContinueOnError})
}

// UsageHandler serves the cost attribution registry.
func UsageHandler() http.Handler {
	return promhttp.HandlerFor(UsageRegistry, promhttp.HandlerOpts{})
}

// Serve serves `/metrics` and the cost attribution registry path on address.
func Serve(address, usagePath, sidecarURL string) (net.Listener, error) {
	RegisterProcessMetrics()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("listen for metrics on %s: %w", address, err)
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", Handler(sidecarURL))
	if usagePath != "" && usagePath != "/metrics" {
		mux.Handle(usagePath, UsageHandler())
	}
	log.Printf("phase=metrics_listen address=%s usage_path=%s", address, usagePath)
	go func() {
		if err := http.Serve(listener, mux); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Printf("metrics server failed: %v", err)
		}
	}()
	return listener, nil
}
