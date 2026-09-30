// SPDX-License-Identifier: AGPL-3.0-only

// Package kafka consumes one partition of the ingest topic.
package kafka

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/grafana/mimir/pkg/storage/seriesstore/record"
)

type OffsetAt int

const (
	Earliest OffsetAt = iota
	Latest
)

type StartOffsetKind int

const (
	StartEarliest StartOffsetKind = iota
	StartLatest
	StartAt
)

type StartOffset struct {
	Kind   StartOffsetKind
	Offset int64
}

// Record is a fetched record, decoded.
type Record struct {
	TimestampMs int64
	Tenant      string
	// Nil when the record has no payload.
	Request *record.DecodedRequest
	Err     error
}

type RecordAndOffset struct {
	Offset int64
	Record Record
}

// RawRecord is a fetched record whose payload has not been decoded yet.
type RawRecord struct {
	Offset      int64
	TimestampMs int64
	Tenant      string
	Version     uint32
	// Nil when the record has no payload.
	Payload []byte
}

// Decode decodes the payload, or reports false when there is none.
func (r *RawRecord) Decode() (record.DecodedRequest, bool, error) {
	if r.Payload == nil {
		return record.DecodedRequest{}, false, nil
	}
	request, err := record.DecodeRecordBytes(r.Version, r.Payload)
	return request, true, err
}

// DecodeWithLabelSpans is like Decode, with where each series' labels are in the payload.
func (r *RawRecord) DecodeWithLabelSpans() (record.DecodedRequest, []record.Span, bool, error) {
	if r.Payload == nil {
		return record.DecodedRequest{}, nil, false, nil
	}
	request, spans, err := record.DecodeRecordWithLabelSpans(r.Version, r.Payload)
	return request, spans, true, err
}

// Config is how to reach the brokers. The SASL password is never printed.
type Config struct {
	Brokers       string
	Topic         string
	Partition     int32
	TLS           bool
	SASLUsername  string
	SASLPassword  string
	SASLMechanism string
}

type PartitionClient struct {
	client        *kgo.Client
	topic         string
	partition     int32
	highWatermark atomic.Int64

	// Records of the last fetch not returned yet.
	mu      sync.Mutex
	pending []*kgo.Record
}

var closedOnOwnGoroutine atomic.Int64

// Close closes the client on its own goroutine: closing waits on the brokers, which can take
// forever during an outage, and once stopped a pod's consumption and later its shutdown.
func (c *PartitionClient) Close() {
	closedOnOwnGoroutine.Add(1)
	go c.client.Close()
}

type logger struct {
	partition int32
	debug     bool
}

func (l logger) Level() kgo.LogLevel {
	if l.debug {
		return kgo.LogLevelDebug
	}
	return kgo.LogLevelWarn
}

func (l logger) Log(level kgo.LogLevel, message string, keyvals ...any) {
	log.Printf("phase=kafka_client_log partition=%d level=%s message=%q %v", l.partition, level, message, keyvals)
}

// options are the client's settings, from the environment like the Rust ingester's.
func options(config Config, clientID string, fetchMaxBytes int32, debug bool) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(splitBrokers(config.Brokers)...),
		kgo.ClientID(clientID),
		kgo.FetchMaxBytes(fetchMaxBytes),
		// Otherwise each single-partition fetch is capped at a few records, which makes catch-up
		// bound by broker round trips.
		kgo.FetchMaxPartitionBytes(fetchMaxBytes),
		kgo.FetchMaxWait(500 * time.Millisecond),
		kgo.FetchIsolationLevel(kgo.ReadUncommitted()),
		// Out of range offsets are errors: consumption resumes from the local segment log.
		kgo.ConsumeResetOffset(kgo.NoResetOffset()),
		kgo.WithLogger(logger{partition: config.Partition, debug: debug}),
	}
	if config.TLS {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{}))
	}
	switch {
	case config.SASLUsername != "" && config.SASLPassword != "":
		mechanism, err := saslMechanism(config)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.SASL(mechanism))
	case config.SASLUsername != "" || config.SASLPassword != "":
		return nil, errors.New("both SASL username and password are required")
	}
	return opts, nil
}

func saslMechanism(config Config) (sasl.Mechanism, error) {
	switch config.SASLMechanism {
	case "plain":
		return plain.Auth{User: config.SASLUsername, Pass: config.SASLPassword}.AsMechanism(), nil
	case "scram-sha-256":
		return scram.Auth{User: config.SASLUsername, Pass: config.SASLPassword}.AsSha256Mechanism(), nil
	case "scram-sha-512":
		return scram.Auth{User: config.SASLUsername, Pass: config.SASLPassword}.AsSha512Mechanism(), nil
	}
	return nil, fmt.Errorf("unsupported SASL mechanism %s", config.SASLMechanism)
}

func splitBrokers(brokers string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(brokers); i++ {
		if i == len(brokers) || brokers[i] == ',' {
			if i > start {
				out = append(out, brokers[start:i])
			}
			start = i + 1
		}
	}
	return out
}

func Connect(config Config) (*PartitionClient, error) {
	clientID := os.Getenv("MIMIR_KAFKA_CLIENT_ID")
	if clientID == "" {
		clientID = "go-kafka-ingester"
	}
	fetchMaxBytes := int32(8 << 20)
	if value := os.Getenv("MIMIR_KAFKA_FETCH_MAX_BYTES"); value != "" {
		parsed, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("parse MIMIR_KAFKA_FETCH_MAX_BYTES: %w", err)
		}
		fetchMaxBytes = int32(parsed)
	}
	opts, err := options(config, clientID, fetchMaxBytes, os.Getenv("MIMIR_KAFKA_DEBUG") == "true")
	if err != nil {
		return nil, err
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create Kafka consumer: %w", err)
	}
	c := &PartitionClient{client: client, topic: config.Topic, partition: config.Partition}
	c.highWatermark.Store(-1)
	return c, nil
}

// ConsumerGroup is the group the client commits to, only for lag monitoring: restarts resume
// from the local segment checkpoint.
const ConsumerGroup = "go-kafka-ingester"

// Commit commits the next offset to consume for this partition, without waiting for it.
func (c *PartitionClient) Commit(nextOffset int64) error {
	offsets := kadm.Offsets{}
	offsets.Add(kadm.Offset{Topic: c.topic, Partition: c.partition, At: nextOffset, LeaderEpoch: -1})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		responses, err := kadm.NewClient(c.client).CommitOffsets(ctx, ConsumerGroup, offsets)
		if err == nil {
			err = responses.Error()
		}
		if err != nil {
			log.Printf("phase=kafka_commit_failed partition=%d error=%v", c.partition, err)
		}
	}()
	return nil
}

// Assign makes the client fetch from offset, dropping what it had fetched.
func (c *PartitionClient) Assign(offset int64) error {
	c.client.RemoveConsumePartitions(map[string][]int32{c.topic: {c.partition}})
	c.mu.Lock()
	c.pending = nil
	c.mu.Unlock()
	c.client.AddConsumePartitions(map[string]map[int32]kgo.Offset{
		c.topic: {c.partition: kgo.NewOffset().At(offset)},
	})
	return nil
}

// OffsetForTime returns the first offset whose record timestamp is at or after timestampMs, or
// false when every record is older.
func (c *PartitionClient) OffsetForTime(ctx context.Context, timestampMs int64) (int64, bool, error) {
	partition, err := c.listOffset(ctx, timestampMs)
	if err != nil {
		return 0, false, fmt.Errorf("look up Kafka offset for timestamp: %w", err)
	}
	if partition.Offset < 0 {
		return 0, false, nil
	}
	return partition.Offset, true, nil
}

// GetOffset returns the partition's earliest offset or its high watermark.
func (c *PartitionClient) GetOffset(ctx context.Context, at OffsetAt) (int64, error) {
	timestamp := int64(-2)
	if at == Latest {
		timestamp = -1
	}
	partition, err := c.listOffset(ctx, timestamp)
	if err != nil {
		return 0, fmt.Errorf("fetch Kafka watermarks: %w", err)
	}
	if at == Latest {
		c.highWatermark.Store(partition.Offset)
	}
	return partition.Offset, nil
}

func (c *PartitionClient) listOffset(ctx context.Context, timestamp int64) (kmsg.ListOffsetsResponseTopicPartition, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	request := kmsg.NewPtrListOffsetsRequest()
	request.ReplicaID = -1
	topic := kmsg.NewListOffsetsRequestTopic()
	topic.Topic = c.topic
	partition := kmsg.NewListOffsetsRequestTopicPartition()
	partition.Partition = c.partition
	partition.Timestamp = timestamp
	partition.CurrentLeaderEpoch = -1
	topic.Partitions = append(topic.Partitions, partition)
	request.Topics = append(request.Topics, topic)
	response, err := request.RequestWith(ctx, c.client)
	if err != nil {
		return kmsg.ListOffsetsResponseTopicPartition{}, err
	}
	for _, topic := range response.Topics {
		for _, partition := range topic.Partitions {
			if topic.Topic == c.topic && partition.Partition == c.partition {
				if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
					return kmsg.ListOffsetsResponseTopicPartition{}, err
				}
				return partition, nil
			}
		}
	}
	return kmsg.ListOffsetsResponseTopicPartition{}, errors.New("Kafka offset lookup returned no partition")
}

// NextRaw returns the next record and the partition's high watermark as known.
func (c *PartitionClient) NextRaw(ctx context.Context) (RawRecord, int64, error) {
	next, err := c.next(ctx)
	if err != nil {
		return RawRecord{}, 0, err
	}
	raw := RawRecord{
		Offset:      next.Offset,
		TimestampMs: next.Timestamp.UnixMilli(),
		Tenant:      string(next.Key),
		Version:     RecordVersion(next.Headers),
		Payload:     next.Value,
	}
	return raw, max(c.highWatermark.Load(), next.Offset+1), nil
}

// Next returns the next record decoded, and the partition's high watermark as known.
func (c *PartitionClient) Next(ctx context.Context) (RecordAndOffset, int64, error) {
	raw, highWatermark, err := c.NextRaw(ctx)
	if err != nil {
		return RecordAndOffset{}, 0, err
	}
	decoded := Record{TimestampMs: raw.TimestampMs, Tenant: raw.Tenant}
	if raw.Payload != nil {
		request, err := record.DecodeRecord(raw.Version, raw.Payload)
		decoded.Request, decoded.Err = &request, err
	}
	return RecordAndOffset{Offset: raw.Offset, Record: decoded}, highWatermark, nil
}

func (c *PartitionClient) next(ctx context.Context) (*kgo.Record, error) {
	for {
		c.mu.Lock()
		if len(c.pending) > 0 {
			next := c.pending[0]
			c.pending = c.pending[1:]
			c.mu.Unlock()
			return next, nil
		}
		c.mu.Unlock()
		fetches := c.client.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var fetchErr error
		fetches.EachError(func(_ string, _ int32, err error) {
			if fetchErr == nil && !errors.Is(err, context.Canceled) {
				fetchErr = err
			}
		})
		records := fetches.Records()
		c.mu.Lock()
		c.pending = append(c.pending, records...)
		c.mu.Unlock()
		if fetchErr != nil && len(records) == 0 {
			return nil, fetchErr
		}
	}
}

// RecordVersion is the record's ingest-storage version, like Go's ParseRecordVersion: the first
// Version header, and 0 without a valid one.
func RecordVersion(headers []kgo.RecordHeader) uint32 {
	for _, header := range headers {
		if header.Key == "Version" {
			if len(header.Value) != 4 {
				return 0
			}
			return binary.BigEndian.Uint32(header.Value)
		}
	}
	return 0
}
