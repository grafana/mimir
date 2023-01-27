package aggregator

import (
	"context"
	"flag"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
)

func TestStartStopBatcher(t *testing.T) {
	f := flag.NewFlagSet("", flag.PanicOnError)
	cfg := Config{}
	cfg.RegisterFlags(f) // Set default config.

	batcher := NewBatcher(Config{
		FlushInterval: time.Second,
	}, nil, log.NewNopLogger())

	require.NoError(t, batcher.starting(context.Background()))
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, batcher.stopping(nil))
}
