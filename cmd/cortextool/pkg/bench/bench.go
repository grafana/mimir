package bench

import (
	"context"
	"flag"
	"os"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v2"
)

type Config struct {
	ID               string `yaml:"id"`
	InstanceName     string `yaml:"instance_name"`
	WorkloadFilePath string `yaml:"workload_file_path"`

	RingCheck RingCheckConfig  `yaml:"ring_check"`
	Write     WriteBenchConfig `yaml:"writes"`
	Query     QueryConfig      `yaml:"query"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	defaultID, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	f.StringVar(&cfg.ID, "bench.id", defaultID, "ID of worker. Defaults to hostname")
	f.StringVar(&cfg.InstanceName, "bench.instance-name", "default", "Instance name writes and queries will be run against.")
	f.StringVar(&cfg.WorkloadFilePath, "bench.workload-file-path", "./workload.yaml", "path to the file containing the workload description")

	cfg.Write.RegisterFlags(f)
	cfg.Query.RegisterFlags(f)
	cfg.RingCheck.RegisterFlagsWithPrefix("bench.ring-check.", f)
}

type Runner struct {
	cfg Config

	writeRunner     *WriteBenchmarkRunner
	queryRunner     *queryRunner
	ringCheckRunner *RingChecker
}

func NewBenchRunner(cfg Config, logger log.Logger, reg prometheus.Registerer) (*Runner, error) {
	// Load workload file

	content, err := os.ReadFile(cfg.WorkloadFilePath)
	if err != nil {
		return nil, errors.Wrap(err, "unable to read workload YAML file from the disk")
	}

	workloadDesc := WorkloadDesc{}
	err = yaml.Unmarshal(content, &workloadDesc)
	if err != nil {
		return nil, errors.Wrap(err, "unable to unmarshal workload YAML file")
	}

	level.Info(logger).Log("msg", "building workload")
	workload := newWriteWorkload(workloadDesc, prometheus.DefaultRegisterer)

	benchRunner := &Runner{
		cfg: cfg,
	}

	if cfg.Write.Enabled {
		benchRunner.writeRunner, err = NewWriteBenchmarkRunner(cfg.ID, cfg.InstanceName, cfg.Write, workload, logger, reg)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create write benchmarker")
		}
	}

	if cfg.RingCheck.Enabled {
		benchRunner.ringCheckRunner, err = NewRingChecker(cfg.ID, cfg.InstanceName, cfg.RingCheck, workload, logger)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create ring checker")
		}
	}

	if cfg.Query.Enabled {
		queryWorkload, err := newQueryWorkload(cfg.ID, workloadDesc)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create query benchmark workload")
		}
		benchRunner.queryRunner, err = newQueryRunner(cfg.ID, cfg.InstanceName, cfg.Query, queryWorkload, logger, reg)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create query benchmark runner")
		}
	}
	return benchRunner, nil
}

func (b *Runner) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	if b.writeRunner != nil {
		g.Go(func() error {
			return b.writeRunner.Run(ctx)
		})
	}

	if b.ringCheckRunner != nil {
		g.Go(func() error {
			return b.ringCheckRunner.Run(ctx)
		})
	}

	if b.queryRunner != nil {
		g.Go(func() error {
			return b.queryRunner.Run(ctx)
		})
	}
	return g.Wait()
}
