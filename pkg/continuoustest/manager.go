// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"context"
	"flag"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/modules"
	"golang.org/x/sync/errgroup"
)

type Test interface {
	// Name returns the test name. The name is used to uniquely identify the test in metrics and logs.
	Name() string

	// Init initializes the test. If the initialization fails, the testing tool will terminate.
	Init(ctx context.Context, now time.Time) error

	// Run runs a single test cycle. This function is called multiple times, at periodic intervals.
	// The returned error is ignored unless smoke-test is enabled. In that case, the error is returned to the caller.
	Run(ctx context.Context, now time.Time) error
}

type ManagerConfig struct {
	SmokeTest   bool
	RunInterval time.Duration
}

func (cfg *ManagerConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.SmokeTest, "tests.smoke-test", false, "Run a smoke test, i.e. run all tests once and exit.")
	f.DurationVar(&cfg.RunInterval, "tests.run-interval", 5*time.Minute, "How frequently tests should run.")
}

type Manager struct {
	cfg    ManagerConfig
	logger log.Logger
	tests  []Test
}

func NewManager(cfg ManagerConfig, logger log.Logger) *Manager {
	return &Manager{
		cfg:    cfg,
		logger: logger,
	}
}

func (m *Manager) AddTest(t Test) {
	m.tests = append(m.tests, t)
}

func (m *Manager) Run(ctx context.Context) error {
	// Initialize all tests.
	for _, t := range m.tests {
		if err := t.Init(ctx, time.Now().UTC()); err != nil {
			return err
		}
	}

	// Continuously run all tests. Each test is executed in a dedicated goroutine.
	group, ctx := errgroup.WithContext(ctx)

	for _, test := range m.tests {
		t := test
		group.Go(func() error {

			// Run it immediately, and then every configured period.
			err := t.Run(ctx, time.Now().UTC())
			if m.cfg.SmokeTest {
				if err != nil {
					level.Info(m.logger).Log("msg", "Test failed", "test", t.Name(), "err", err)
				} else {
					level.Info(m.logger).Log("msg", "Test passed", "test", t.Name())
				}
				return err
			}

			ticker := time.NewTicker(m.cfg.RunInterval)

			for {
				select {
				case <-ticker.C:
					// This error is intentionally ignored because we want to
					// continue running the tests forever.
					_ = t.Run(ctx, time.Now().UTC())
				case <-ctx.Done():
					return nil
				}
			}
		})
	}

	err := group.Wait()
	if err == nil && m.cfg.SmokeTest {
		err = modules.ErrStopProcess
	}
	return err
}
