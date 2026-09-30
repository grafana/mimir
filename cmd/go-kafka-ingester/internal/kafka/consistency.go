// SPDX-License-Identifier: AGPL-3.0-only

package kafka

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Consistency makes reads wait for, or refuse, what Mimir's read consistency asks of the
// partition's consumption, per Kafka cluster.
type Consistency struct {
	partition               int32
	readCompartment         int32
	consumed                []atomic.Int64
	highWatermark           []atomic.Int64
	lastConsumedTimestampMs []atomic.Int64
	timeout                 time.Duration

	mu      sync.RWMutex
	clients []*PartitionClient
	// Closed and replaced whenever consumption moves, to wake the reads waiting on it.
	changed chan struct{}
}

func NewConsistency(partition, readCompartment int32, clusters int, timeout time.Duration) *Consistency {
	c := &Consistency{
		partition:               partition,
		readCompartment:         readCompartment,
		consumed:                make([]atomic.Int64, clusters),
		highWatermark:           make([]atomic.Int64, clusters),
		lastConsumedTimestampMs: make([]atomic.Int64, clusters),
		timeout:                 timeout,
		clients:                 make([]*PartitionClient, clusters),
		changed:                 make(chan struct{}),
	}
	for i := range clusters {
		c.consumed[i].Store(-1)
		c.highWatermark[i].Store(-1)
	}
	return c
}

func fetchMax(value *atomic.Int64, candidate int64) {
	for {
		current := value.Load()
		if candidate <= current || value.CompareAndSwap(current, candidate) {
			return
		}
	}
}

func (c *Consistency) Consumed(cluster int, offset, highWatermark, timestampMs int64) {
	c.consumed[cluster].Store(offset)
	fetchMax(&c.lastConsumedTimestampMs[cluster], timestampMs)
	c.HighWatermark(cluster, highWatermark)
}

func (c *Consistency) HighWatermark(cluster int, highWatermark int64) {
	fetchMax(&c.highWatermark[cluster], highWatermark-1)
	c.mu.Lock()
	close(c.changed)
	c.changed = make(chan struct{})
	c.mu.Unlock()
}

func (c *Consistency) SetClient(cluster int, client *PartitionClient) {
	c.mu.Lock()
	c.clients[cluster] = client
	c.mu.Unlock()
}

func first(md metadata.MD, key string) string {
	if values := md.Get(key); len(values) > 0 {
		return values[0]
	}
	return ""
}

// Enforce returns once the read in ctx may run, or the gRPC error to answer it with.
func (c *Consistency) Enforce(ctx context.Context) error {
	md, _ := metadata.FromIncomingContext(ctx)
	level := first(md, "__consistency_level__")
	if level != "strong" {
		return c.enforceMaxDelay(md)
	}
	targets, ok := offsetsFor(first(md, "__consistency_offsets__"), c.readCompartment, c.partition)
	if !ok || len(targets) != len(c.consumed) {
		c.mu.RLock()
		clients := append([]*PartitionClient(nil), c.clients...)
		c.mu.RUnlock()
		all := true
		for _, client := range clients {
			all = all && client != nil
		}
		targets = make([]int64, 0, len(clients))
		if all {
			for _, client := range clients {
				offset, err := client.GetOffset(ctx, Latest)
				if err != nil {
					return status.Error(codes.Unavailable, err.Error())
				}
				targets = append(targets, offset-1)
			}
		} else {
			for i := range c.highWatermark {
				targets = append(targets, c.highWatermark[i].Load())
			}
		}
	}
	deadline := time.NewTimer(c.timeout)
	defer deadline.Stop()
	for {
		c.mu.RLock()
		changed := c.changed
		c.mu.RUnlock()
		if c.reached(targets) {
			return nil
		}
		select {
		case <-changed:
		case <-deadline.C:
			return status.Error(codes.DeadlineExceeded, "waiting for ingest-storage read consistency")
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		}
	}
}

func (c *Consistency) reached(targets []int64) bool {
	for i, target := range targets {
		if target >= 0 && c.consumed[i].Load() < target {
			return false
		}
	}
	return true
}

func (c *Consistency) enforceMaxDelay(md metadata.MD) error {
	value := first(md, "__consistency_max_delay__")
	if value == "" {
		return nil
	}
	maxDelay, err := time.ParseDuration(value)
	if err != nil {
		return nil
	}
	now := time.Now().UnixMilli()
	for i := range c.consumed {
		timestamp := c.lastConsumedTimestampMs[i].Load()
		if c.consumed[i].Load() < c.highWatermark[i].Load() && timestamp > 0 && now-timestamp > maxDelay.Milliseconds() {
			return status.Error(codes.Unavailable, "partition reader exceeds the allowed read delay")
		}
	}
	return nil
}

// offsetsFor reads the partition's offsets from Mimir's encodings of them.
func offsetsFor(encoded string, readCompartment, partition int32) ([]int64, bool) {
	if entries, ok := strings.CutPrefix(encoded, "v1="); ok {
		value, ok := findValue(entries, strconv.Itoa(int(partition))+":")
		if !ok {
			return nil, false
		}
		offset, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, false
		}
		return []int64{offset}, true
	}
	entries, ok := strings.CutPrefix(encoded, "v2=")
	if !ok {
		return nil, false
	}
	value, ok := findValue(entries, strconv.Itoa(int(readCompartment))+"/"+strconv.Itoa(int(partition))+":")
	if !ok {
		return nil, false
	}
	var offsets []int64
	for _, part := range strings.Split(value, ";") {
		offset, err := strconv.ParseInt(part, 10, 64)
		if err != nil {
			return nil, false
		}
		offsets = append(offsets, offset)
	}
	return offsets, true
}

func findValue(entries, key string) (string, bool) {
	for _, entry := range strings.Split(entries, ",") {
		if value, ok := strings.CutPrefix(entry, key); ok {
			return value, true
		}
	}
	return "", false
}
