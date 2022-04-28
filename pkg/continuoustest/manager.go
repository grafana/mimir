// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"context"
	"flag"
	"sync"
	"time"
)

type Test interface {
	// Name returns the test name. The name is used to uniquely identify the test in metrics and logs.
	Name() string

	// Init initializes the test. If the initialization fails, the testing tool will terminate.
	Init(ctx context.Context, now time.Time) error

	// Run runs a single test cycle. This function is called multiple times, at periodic intervals.
	Run(ctx context.Context, now time.Time)
}

type ManagerConfig struct {
	RunInterval time.Duration
}

func (cfg *ManagerConfig) RegisterFlags(f *flag.FlagSet) {
	f.DurationVar(&cfg.RunInterval, "tests.run-interval", 5*time.Minute, "How frequently tests should run.")
}

type Manager struct {
	cfg   ManagerConfig
	tests []Test
}

func NewManager(cfg ManagerConfig) *Manager {
	return &Manager{
		cfg: cfg,
	}
}

func (m *Manager) AddTest(t Test) {
	m.tests = append(m.tests, t)
}

func (m *Manager) Run(ctx context.Context) error {
	// Initialize all tests.
	for _, t := range m.tests {
		if err := t.Init(ctx, time.Now()); err != nil {
			return err
		}
	}

	// Continuously run all tests. Each test is executed in a dedicated goroutine.
	wg := sync.WaitGroup{}
	wg.Add(len(m.tests))

	for _, test := range m.tests {
		go func(t Test) {
			defer wg.Done()

			// Run it immediately, and then every configured period.
			t.Run(ctx, time.Now())

			ticker := time.NewTicker(m.cfg.RunInterval)

			for {
				select {
				case <-ticker.C:
					t.Run(ctx, time.Now())
				case <-ctx.Done():
					return
				}
			}
		}(test)
	}

	wg.Wait()
	return nil
}
