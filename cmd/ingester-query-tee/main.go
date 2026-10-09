// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/grpcclient"
	"github.com/grafana/dskit/log"
	"github.com/grafana/dskit/server"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/util/grpcencoding/s2"
	"github.com/grafana/mimir/tools/ingesterquerytee"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	var cfg ingesterquerytee.Config
	var serverCfg server.Config
	var primaryCfg, shadowCfg grpcclient.Config
	cfg.RegisterFlags(flag.CommandLine)
	serverCfg.RegisterFlags(flag.CommandLine)
	primaryCfg.CustomCompressors = []string{s2.Name}
	shadowCfg.CustomCompressors = []string{s2.Name}
	primaryCfg.RegisterFlagsWithPrefix("primary", flag.CommandLine)
	shadowCfg.RegisterFlagsWithPrefix("shadow", flag.CommandLine)
	if err := flag.CommandLine.Set("server.http-listen-port", "9900"); err != nil {
		return err
	}
	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		return err
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	if err := primaryCfg.Validate(); err != nil {
		return err
	}
	if err := shadowCfg.Validate(); err != nil {
		return err
	}
	primaryOpts, err := primaryCfg.DialOption(nil, nil, nil)
	if err != nil {
		return err
	}
	shadowOpts, err := shadowCfg.DialOption(nil, nil, nil)
	if err != nil {
		return err
	}
	primary, err := grpc.NewClient(cfg.PrimaryAddress, primaryOpts...)
	if err != nil {
		return err
	}
	defer primary.Close()
	shadow, err := grpc.NewClient(cfg.ShadowAddress, shadowOpts...)
	if err != nil {
		return err
	}
	defer shadow.Close()
	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	logger := log.NewGoKitWithLevel(serverCfg.LogLevel, serverCfg.LogFormat)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	proxy, err := ingesterquerytee.New(ctx, cfg, primary, shadow, registry, logger)
	if err != nil {
		return err
	}
	serverCfg.Registerer = registry
	serverCfg.Gatherer = registry
	serverCfg.MetricsNamespace = "ingester_query_tee"
	serverCfg.Log = logger
	serverCfg.GRPCOptions = append(serverCfg.GRPCOptions, grpc.ForceServerCodec(ingesterquerytee.WireCodec{}), grpc.UnknownServiceHandler(proxy.Handler))
	srv, err := server.New(serverCfg)
	if err != nil {
		return err
	}
	defer srv.Shutdown()
	srv.HTTP.Path("/ready").HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return srv.Run()
}
