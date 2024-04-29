// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"context"
	"flag"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/modules"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type dummyTest struct {
	runs int
	err  error
}

// Name implements Test.
func (d *dummyTest) Name() string {
	return "dummyTest"
}

// Init implements Test.
func (d *dummyTest) Init(_ context.Context, _ time.Time) error {
	return nil
}

// Run implements Test.
func (d *dummyTest) Run(_ context.Context, _ time.Time) error {
	d.runs++
	return d.err
}

func TestManager_PeriodicRun(t *testing.T) {
	logger := log.NewNopLogger()
	cfg := ManagerConfig{}
	cfg.RegisterFlags(flag.NewFlagSet("", flag.ContinueOnError))
	cfg.RunInterval = time.Millisecond * 10

	manager := NewManager(cfg, logger)

	dummyTest := &dummyTest{}
	manager.AddTest(dummyTest)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
	defer cancel()
	err := manager.Run(ctx)

	require.NoError(t, err)
	// Theoretically the test will run 6 times, but small timing differences may
	// cause it to run only 5 times.
	require.GreaterOrEqual(t, dummyTest.runs, 5)
}

func TestManager_SmokeTest(t *testing.T) {
	t.Run("successful smoke test", func(t *testing.T) {
		logger := log.NewNopLogger()
		cfg := ManagerConfig{}
		cfg.RegisterFlags(flag.NewFlagSet("", flag.ContinueOnError))
		cfg.RunInterval = time.Millisecond * 10
		cfg.SmokeTest = true

		manager := NewManager(cfg, logger)

		dummyTest := &dummyTest{}
		manager.AddTest(dummyTest)

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		defer cancel()
		err := manager.Run(ctx)

		require.EqualError(t, err, modules.ErrStopProcess.Error())
		require.Equal(t, dummyTest.runs, 1)
	})

	t.Run("failed smoke test", func(t *testing.T) {
		logger := log.NewNopLogger()
		cfg := ManagerConfig{}
		cfg.RegisterFlags(flag.NewFlagSet("", flag.ContinueOnError))
		cfg.RunInterval = time.Millisecond * 10
		cfg.SmokeTest = true

		manager := NewManager(cfg, logger)

		dummyTest := &dummyTest{}
		dummyTest.err = errors.New("test error")
		manager.AddTest(dummyTest)

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		defer cancel()
		err := manager.Run(ctx)

		require.ErrorIs(t, err, dummyTest.err)
		require.Equal(t, dummyTest.runs, 1)
	})
}
