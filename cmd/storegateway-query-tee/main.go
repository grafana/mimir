// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"

	util_log "github.com/grafana/mimir/pkg/util/log"
	grpctee "github.com/grafana/mimir/tools/grpc-tee"
)

func main() {
	// Cleanup all flags registered via init() methods of 3rd-party libraries.
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	var cfg grpctee.Config
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

	backend := NewStoreGatewayBackend()
	tee := grpctee.NewTee(backend, cfg.Server.Log)

	cfg.Server.GRPCOptions = append(
		cfg.Server.GRPCOptions,
		// The frame codec, handles messages as raw bytes.
		grpc.ForceServerCodec(grpctee.FrameCodec{}),
		// The server registers no services, so every call goes to the tee handler.
		grpc.UnknownServiceHandler(tee.Handle),
	)

	teeServer := grpctee.NewTeeServer(tee)

	teeService, err := teeServer.Init(cfg)
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
