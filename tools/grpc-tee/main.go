// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/server"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/grafana/mimir/pkg/mimir"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

func main() {
	// Cleanup all flags registered via init() methods of 3rd-party libraries.
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	var cfg Config
	// Sets default values from flags to the config.
	cfg.RegisterFlags(flag.CommandLine, util_log.Logger)

	flag.CommandLine.Init(flag.CommandLine.Name(), flag.ContinueOnError)

	// Parse before the logger init, so that the log flags take effect.
	if err := flag.CommandLine.Parse(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			exit(0)
		}
		fmt.Fprintln(flag.CommandLine.Output(), "Run with -help to get a list of available parameters")
		exit(2)
	}

	reg := prometheus.DefaultRegisterer
	cfg.Server.Log = util_log.InitLogger(
		cfg.Server.LogFormat,
		cfg.Server.LogLevel,
		true,
		util_log.RateLimitedLoggerCfg{Registry: reg},
	)

	tee := &GRPCTee{}
	teeService, err := tee.initServer(cfg)
	util_log.CheckFatal("Init gRPC tee service", err)

	err = services.StartAndAwaitRunning(context.Background(), teeService)
	util_log.CheckFatal("Start gRPC tee service", err)

	err = teeService.AwaitTerminated(context.Background())
	if err != nil {
		exit(1)
	}
}

func exit(code int) {
	if err := util_log.Flush(); err != nil {
		fmt.Fprintln(os.Stderr, "Could not flush logger", err)
	}
	os.Exit(code)
}

type GRPCTee struct {
	srv        *server.Server
	srvMetrics *server.Metrics
}

// initServer is a simplified version of Mimir's modules initServer
func (t *GRPCTee) initServer(cfg Config) (services.Service, error) {
	var err error

	t.srvMetrics = server.NewServerMetrics(cfg.Server)
	t.srv, err = server.NewWithMetrics(cfg.Server, t.srvMetrics)
	if err != nil {
		return nil, err
	}

	servicesToWaitFor := func() []services.Service {
		return nil // TBD
	}

	s := mimir.NewServerService(t.srv, servicesToWaitFor)

	// The tee is ready while its only service is Running.
	// The state changes to Stopping on shutdown.
	isRunning := func(context.Context) bool { return s.State() == services.Running }

	// Health check endpoint.
	t.srv.HTTP.Path("/").Methods("GET").HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Readiness check endpoint.
	t.srv.HTTP.Path("/ready").Methods("GET").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isRunning(r.Context()) {
			http.Error(w, "Application is not running", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{\"status\": \"ready\"}"))
	})
	grpc_health_v1.RegisterHealthServer(t.srv.GRPC, grpcutil.NewHealthCheckFrom(isRunning))

	return s, nil
}
