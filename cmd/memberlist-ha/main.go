// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"time"

	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/codec"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/thanos-io/thanos/pkg/discovery/dns"
	"github.com/weaveworks/common/logging"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/common/signals"

	"github.com/grafana/mimir/pkg/mimir"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

type stopperFunc func() error

func (f stopperFunc) Stop() error {
	return f()
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	store, memberlistSvc := setupStore()
	_, serverSvc := setupServer()

	manager, err := services.NewManager(serverSvc, memberlistSvc)
	if err != nil {
		panic(err)
	}
	stopServices := stopperFunc(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
		defer cancel()
		manager.StopAsync()
		return manager.AwaitStopped(ctx)
	})
	cancelContext := stopperFunc(func() error {
		cancel()
		return nil
	})
	signalHandler := signals.NewHandler(logging.GoKit(util_log.Logger), stopServices, cancelContext)
	err = manager.StartAsync(context.Background())
	if err != nil {
		panic(err)
	}
	go runExperiment(ctx, store)

	signalHandler.Loop()
}

func setupServer() (*server.Server, services.Service) {
	serv, err := server.New(*serverConfig)
	if err != nil {
		panic(err)
	}

	serv.HTTP.Path("/metrics").Handler(promhttp.Handler())
	return serv, mimir.NewServerService(serv, nil)
}

func setupStore() (kv.Client, services.Service) {
	reg := prometheus.DefaultRegisterer

	dnsProvider := dns.NewProvider(util_log.Logger, reg, dns.GolangResolverType)
	memberlistConfig.MetricsRegisterer = reg
	memberlistConfig.Codecs = []codec.Codec{valueCodec{}}

	memberlistKV := memberlist.NewKVInitService(memberlistConfig, util_log.Logger, dnsProvider, reg)

	cfg := kv.Config{
		Store: "memberlist",
		StoreConfig: kv.StoreConfig{
			MemberlistKV: memberlistKV.GetMemberlistKV,
		},
	}
	store, err := kv.NewClient(
		cfg,
		memberlistConfig.Codecs[0],
		kv.RegistererWithKVName(reg, "test-kv-store"),
		util_log.Logger,
	)
	if err != nil {
		panic(err)
	}

	return store, memberlistKV
}
