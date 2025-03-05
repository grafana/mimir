package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"

	"github.com/grafana/dskit/flagext"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/user"
	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/prometheus/client_golang/prometheus"
)

type config struct {
	IP          string
	Parallelism int
	Count       int

	ic ingester_client.Config
}

func main() {
	cfg := config{}

	cfg.ic.RegisterFlags(flag.CommandLine)

	flag.StringVar(&cfg.IP, "ip", "", "ingester IP")
	flag.IntVar(&cfg.Parallelism, "p", 1, "parallelism")
	flag.IntVar(&cfg.Count, "c", 1000, "count")
	flag.Usage = func() {
		fmt.Fprintln(flag.CommandLine.Output(), os.Args[0], "is a tool to update meta.json files to conform to Mimir requirements.")
		fmt.Fprintln(flag.CommandLine.Output(), "Flags:")
		flag.PrintDefaults()
	}

	println("DFFFF")

	// Parse CLI arguments.
	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		panic(err.Error())
	}

	r := prometheus.NewRegistry()
	met := ingester_client.NewMetrics(r)

	var wg sync.WaitGroup
	wg.Add(cfg.Parallelism)

	for range cfg.Parallelism {
		go func() {
			fire(cfg, met)
			wg.Done()
		}()
	}
	wg.Wait()
}

func fire(cfg config, m *ingester_client.Metrics) {
	for range cfg.Count {
		inst := ring.InstanceDesc{
			Addr:  cfg.IP,
			State: ring.ACTIVE,
			Id:    "g",
		}

		cc, err := ingester_client.MakeIngesterClient(inst, cfg.ic, m)
		if err != nil {
			panic(err)
		}

		ctx := user.InjectOrgID(context.Background(), "1")

		_, err = cc.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
		if err != nil {
			fmt.Printf("error in healthcheck: %s\n", err)
		}
		cc.Close()
	}
}
