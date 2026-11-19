// SPDX-License-Identifier: AGPL-3.0-only

// native-metadata-prober probes the native metadata prototype on a cell's experiment tenants: it compares info()
// with native target_info and with the target_info join, checks the native metadata lookups, and serves the counts
// as metrics.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/grafana/mimir/pkg/nativemetadataprober"
)

func main() {
	var (
		cfg           nativemetadataprober.Config
		address       string
		timeout       time.Duration
		listenAddress string
	)
	flag.StringVar(&address, "address", "", "The cell's Prometheus API, with its prefix, through a gateway that takes the tenant from the X-Scope-OrgID header, such as http://gateway/prometheus.")
	flag.DurationVar(&timeout, "timeout", 30*time.Second, "Timeout of each query and lookup.")
	flag.StringVar(&listenAddress, "listen-address", ":8080", "Address to serve /metrics on.")
	cfg.RegisterFlags(flag.CommandLine)
	logger := level.NewFilter(log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr)), level.AllowInfo())
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)

	if err := run(cfg, address, timeout, listenAddress, logger); err != nil {
		level.Error(logger).Log("msg", "native-metadata-prober failed", "err", err)
		os.Exit(1)
	}
}

func run(cfg nativemetadataprober.Config, address string, timeout time.Duration, listenAddress string, logger log.Logger) error {
	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		return err
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	client, err := nativemetadataprober.NewHTTPClient(address, timeout)
	if err != nil {
		return err
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	prober := nativemetadataprober.NewProber(cfg, client, logger, reg)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	server := &http.Server{Addr: listenAddress, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	serverErr := make(chan error, 1)
	go func() { serverErr <- server.ListenAndServe() }()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	level.Info(logger).Log("msg", "probing", "tenants", cfg.Tenants.String(), "queries", len(cfg.Queries), "interval", cfg.Interval)
	go prober.Run(ctx)

	select {
	case <-ctx.Done():
	case err := <-serverErr:
		return fmt.Errorf("serving metrics: %w", err)
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
