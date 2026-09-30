// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/cmd/query-tee/main.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package main

import (
	"context"
	"errors"
	"flag"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"syscall"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/dns"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/siderolabs/grpc-proxy/proxy"
	"google.golang.org/grpc"
)

func main() {
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)

	var (
		memberlistCfg memberlist.KVConfig
		ringCfg       RingConfig
		backendCfg    RingBackendConfig
	)
	listenAddress := flag.String("server.grpc-listen-address", ":9095", "Address to listen on for gRPC requests.")
	httpListenAddress := flag.String("server.http-listen-address", ":8095", "Address to listen on for HTTP requests (ring status page and metrics).")
	memberlistCfg.RegisterFlags(flag.CommandLine)
	ringCfg.RegisterFlags(flag.CommandLine, logger)
	backendCfg.RegisterFlagsWithPrefix("backend.", flag.CommandLine)
	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		fatal(logger, "failed to parse flags", err)
	}

	// Advertise the gRPC port in the ring, unless -ring.instance-port overrides it.
	_, grpcPort, err := net.SplitHostPort(*listenAddress)
	if err != nil {
		fatal(logger, "invalid gRPC listen address", err)
	}
	if ringCfg.Common.ListenPort, err = strconv.Atoi(grpcPort); err != nil {
		fatal(logger, "invalid gRPC listen port", err)
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	cortexReg := prometheus.WrapRegistererWithPrefix("cortex_", reg)

	// Set up the memberlist KV the same way as Mimir does in initMemberlistKV.
	memberlistCfg.Codecs = append(memberlistCfg.Codecs, ring.GetCodec())
	dnsProvider := dns.NewProvider(dns.GolangResolverType, 0, logger, prometheus.WrapRegistererWith(prometheus.Labels{"component": "memberlist"}, cortexReg))
	memberlistKV := memberlist.NewKVInitService(&memberlistCfg, log.With(logger, "component", "memberlist"), dnsProvider, reg)
	ringCfg.Common.KVStore.MemberlistKV = memberlistKV.GetMemberlistKV
	backendCfg.Ring.KVStore.MemberlistKV = memberlistKV.GetMemberlistKV

	lifecycler, err := newLifecycler(ringCfg, logger, cortexReg)
	if err != nil {
		fatal(logger, "failed to create ring lifecycler", err)
	}
	ringClient, err := newRing(ringCfg, logger, cortexReg)
	if err != nil {
		fatal(logger, "failed to create ring client", err)
	}
	backend, err := NewRingBackend(backendCfg, logger, cortexReg)
	if err != nil {
		fatal(logger, "failed to create backend", err)
	}

	// Start the services in dependency order, and stop them in reverse order.
	// The backend starts before the lifecycler, so this instance joins the ring only when it can read the backend ring.
	ringServices := []services.Service{memberlistKV, backend, lifecycler, ringClient}
	for _, s := range ringServices {
		if err := services.StartAndAwaitRunning(context.Background(), s); err != nil {
			fatal(logger, "failed to start service", err)
		}
	}
	level.Info(logger).Log("msg", "joined ring", "ring", ringKey, "instance_id", lifecycler.GetInstanceID(), "instance_addr", lifecycler.GetInstanceAddr())

	director := func(_ context.Context, fullMethodName string) (proxy.Mode, []proxy.Backend, error) {
		level.Debug(logger).Log("msg", "proxying request", "method", fullMethodName, "backend", backend.Name(), "address", backendCfg.Address)
		return proxy.One2One, []proxy.Backend{backend.ProxyBackend()}, nil
	}

	// The server registers no services, so every call goes to the transparent handler.
	// The server also uses the raw codec, so request frames stay opaque bytes.
	grpcOpts := []grpc.ServerOption{
		grpc.ForceServerCodecV2(proxy.Codec()),
		grpc.UnknownServiceHandler(proxy.TransparentHandler(director)),
	}
	if codec := backend.Codec(); codec != nil {
		grpcOpts = append(grpcOpts, grpc.StreamInterceptor(decodingStreamInterceptor(codec, logProxiedCall(logger))))
	}
	grpcServer := grpc.NewServer(grpcOpts...)

	lis, err := net.Listen("tcp", *listenAddress)
	if err != nil {
		fatal(logger, "failed to listen for gRPC", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/ring", ringClient)
	mux.Handle("/memberlist", memberlistKV)
	mux.Handle("/backend/"+backend.Name()+"/ring", backend)
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	httpServer := &http.Server{
		Addr:         *httpListenAddress,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	serveErrs := make(chan error, 2)
	go func() { serveErrs <- grpcServer.Serve(lis) }()
	go func() { serveErrs <- httpServer.ListenAndServe() }()
	level.Info(logger).Log("msg", "grpc-tee started", "grpc_address", *listenAddress, "http_address", *httpListenAddress, "backend", backend.Name(), "backend_address", backendCfg.Address)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	exitCode := 0
	select {
	case sig := <-signals:
		level.Info(logger).Log("msg", "received signal, shutting down", "signal", sig)
	case err := <-serveErrs:
		level.Error(logger).Log("msg", "server failed, shutting down", "err", err)
		exitCode = 1
	}

	grpcServer.Stop()
	if err := httpServer.Close(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		level.Warn(logger).Log("msg", "failed to close HTTP server", "err", err)
	}
	// Stopping the lifecycler removes this instance from the ring.
	for _, s := range slices.Backward(ringServices) {
		if err := services.StopAndAwaitTerminated(context.Background(), s); err != nil {
			level.Warn(logger).Log("msg", "failed to stop service", "err", err)
		}
	}
	os.Exit(exitCode)
}

func fatal(logger log.Logger, msg string, err error) {
	level.Error(logger).Log("msg", msg, "err", err)
	os.Exit(1)
}
