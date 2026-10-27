// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/ingester/client"
)

// userStatsIngester answers every UserStats read with its series count.
type userStatsIngester struct {
	client.IngesterClient
	series uint64
}

func (c userStatsIngester) UserStats(context.Context, *client.UserStatsRequest, ...grpc.CallOption) (*client.UserStatsResponse, error) {
	return &client.UserStatsResponse{NumSeries: c.series}, nil
}

func TestDriverRetriesTheDifferencesFoundBeforeReadsStop(t *testing.T) {
	w := newWorld(defaultProfile().Series, 1_000, 1)
	now := func() int64 { return time.Now().UnixMilli() }
	gen := newQueryGen(w, defaultProfile().Queries, time.Hour.Milliseconds(), now, now)
	ingesters := [2]*ingesterProcess{
		{name: "A", client: userStatsIngester{series: 1}},
		{name: "B", client: userStatsIngester{series: 2}},
	}
	// Retries a while after reads stop, as the window's last differences are.
	d := newDriver(gen, &feed{}, ingesters, 16, 200*time.Millisecond)

	reads, stopReads := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer stopReads()
	d.run(reads, t.Context(), map[string]float64{"UserStats": 200}, 1)
	d.pending.Wait()

	s := d.stats["UserStats"]
	require.NotNil(t, s)
	require.Positive(t, s.compared)
	require.Equal(t, s.compared, s.mismatch, "every read differs and keeps differing: each is a mismatch once retried")
}

func TestWaitReadyReturnsWhenTheIngesterExits(t *testing.T) {
	cmd := exec.Command("false")
	require.NoError(t, cmd.Start())
	p := &ingesterProcess{name: "A", cmd: cmd, exited: make(chan struct{}), httpPort: 1}
	go func() { _ = cmd.Wait(); close(p.exited) }()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	require.ErrorContains(t, p.waitReady(ctx, nil), "A exited: exit status 1")
}
