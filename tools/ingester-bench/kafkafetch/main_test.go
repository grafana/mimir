// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestChooseActiveAndDump(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(4, "ingest"))
	require.NoError(t, err)
	defer cluster.Close()
	producer, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	require.NoError(t, err)
	defer producer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		for ctx.Err() == nil {
			for _, p := range []int32{1, 3} {
				producer.ProduceSync(ctx, &kgo.Record{Topic: "ingest", Partition: p, Value: []byte("x")})
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()
	time.Sleep(time.Second)
	client, err := newClient(cluster.ListenAddrs()[0], "")
	require.NoError(t, err)
	defer client.Close()
	chosen, err := choose(ctx, client, "ingest", "active:2")
	require.NoError(t, err)
	require.Equal(t, []int32{1, 3}, chosen)
	chosen, err = choose(ctx, client, "ingest", "0,2")
	require.NoError(t, err)
	require.Equal(t, []int32{0, 2}, chosen)
	file := filepath.Join(t.TempDir(), "dump.json")
	require.NoError(t, run(ctx, client, "ingest", 1, 50, 1<<30, file))
	f, err := os.Open(file)
	require.NoError(t, err)
	defer f.Close()
	lines := 0
	for s := bufio.NewScanner(f); s.Scan(); {
		lines++
	}
	require.Equal(t, 50, lines)
	// A byte limit that stops the sample short of the end fails it.
	err = run(ctx, client, "ingest", 3, 50, 10, filepath.Join(t.TempDir(), "short.json"))
	require.ErrorContains(t, err, "incomplete")
	// So does the time limit.
	expired, stop := context.WithTimeout(ctx, time.Nanosecond)
	defer stop()
	time.Sleep(time.Millisecond)
	require.Error(t, run(expired, client, "ingest", 3, 50, 1<<30, filepath.Join(t.TempDir(), "late.json")))
}
