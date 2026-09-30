// SPDX-License-Identifier: AGPL-3.0-only

package kafka

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

func clientWith(t *testing.T, config Config, fetchMaxBytes int32) *kgo.Client {
	opts, err := options(config, "client", fetchMaxBytes, false)
	require.NoError(t, err)
	client, err := kgo.NewClient(opts...)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	return client
}

func TestSinglePartitionFetchesAreNotCappedBelowTheFetchSize(t *testing.T) {
	client := clientWith(t, Config{Brokers: "broker:9092"}, 16777216)
	require.Equal(t, int32(16777216), client.OptValue(kgo.FetchMaxBytes))
	require.Equal(t, int32(16777216), client.OptValue(kgo.FetchMaxPartitionBytes))
	// Offsets are only committed explicitly, for lag monitoring.
	require.Equal(t, "", client.OptValue(kgo.ConsumerGroup))
}

// franz-go fetches again as soon as the buffered records are polled, so only the fetch wait
// bounds how long a replay waits on Kafka.
func TestAFullFetchQueueDoesNotStallFetching(t *testing.T) {
	client := clientWith(t, Config{Brokers: "broker:9092"}, 16777216)
	require.Equal(t, 500*time.Millisecond, client.OptValue(kgo.FetchMaxWait))
}

func TestSASLNeedsBothCredentialsAndAKnownMechanism(t *testing.T) {
	_, err := options(Config{Brokers: "b", SASLUsername: "user"}, "client", 1, false)
	require.ErrorContains(t, err, "both SASL username and password are required")
	_, err = options(Config{Brokers: "b", SASLUsername: "user", SASLPassword: "secret", SASLMechanism: "gssapi"}, "client", 1, false)
	require.ErrorContains(t, err, "unsupported SASL mechanism gssapi")
	require.NotContains(t, err.Error(), "secret")
	for _, mechanism := range []string{"plain", "scram-sha-256", "scram-sha-512"} {
		_, err = options(Config{Brokers: "b", SASLUsername: "user", SASLPassword: "secret", SASLMechanism: mechanism}, "client", 1, false)
		require.NoError(t, err)
	}
}

func TestClientsCloseTheirConsumerOnItsOwnGoroutine(t *testing.T) {
	// A broker that accepts connections and never answers, like WarpStream agents during an
	// outage.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		var connections []net.Conn
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			connections = append(connections, connection)
		}
	}()
	client, err := Connect(Config{Brokers: listener.Addr().String(), Topic: "topic"})
	require.NoError(t, err)
	require.NoError(t, client.Assign(0))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_, _, _ = client.NextRaw(ctx)
	cancel()
	closed := closedOnOwnGoroutine.Load()
	started := time.Now()
	client.Close()
	require.Equal(t, closed+1, closedOnOwnGoroutine.Load())
	require.Less(t, time.Since(started), time.Second, "closing the client blocked")
}

func TestFirstVersionHeaderMatchesGo(t *testing.T) {
	version := func(v uint32) []byte { return binary.BigEndian.AppendUint32(nil, v) }
	for _, headers := range [][]kgo.RecordHeader{
		nil,
		{{Key: "Other", Value: []byte("ignored")}},
		{{Key: "Other", Value: []byte("ignored")}, {Key: "Version", Value: version(1)}, {Key: "Version", Value: version(2)}},
		{{Key: "Version", Value: version(2)}},
	} {
		require.Equal(t, uint32(ingest.ParseRecordVersion(&kgo.Record{Headers: headers})), RecordVersion(headers))
	}
	require.Equal(t, uint32(1), RecordVersion([]kgo.RecordHeader{{Key: "Version", Value: version(1)}, {Key: "Version", Value: version(2)}}))
	// A malformed header is version 0 rather than a crash.
	require.Equal(t, uint32(0), RecordVersion([]kgo.RecordHeader{{Key: "Version", Value: []byte{1}}}))
}

func TestReadsMimirOffsetEncodings(t *testing.T) {
	offsets, ok := offsetsFor("v1=1:7,3:42", 0, 3)
	require.True(t, ok)
	require.Equal(t, []int64{42}, offsets)
	offsets, ok = offsetsFor("v2=0/1:7,2/3:42;90", 2, 3)
	require.True(t, ok)
	require.Equal(t, []int64{42, 90}, offsets)
	_, ok = offsetsFor("v2=0/3:42", 2, 3)
	require.False(t, ok)
}

func TestStrongReadsWaitForTheirOffsets(t *testing.T) {
	consistency := NewConsistency(3, 0, 1, time.Second)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"__consistency_level__", "strong",
		"__consistency_offsets__", "v1=3:10",
	))
	done := make(chan error, 1)
	go func() { done <- consistency.Enforce(ctx) }()
	select {
	case err := <-done:
		t.Fatalf("returned before consuming the offset: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	consistency.Consumed(0, 9, 11, time.Now().UnixMilli())
	consistency.Consumed(0, 10, 11, time.Now().UnixMilli())
	require.NoError(t, <-done)

	short := NewConsistency(3, 0, 1, 20*time.Millisecond)
	require.Equal(t, codes.DeadlineExceeded, status.Code(short.Enforce(ctx)))
}

func TestEventualReadsRefuseADelayedPartition(t *testing.T) {
	consistency := NewConsistency(0, 0, 1, time.Second)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("__consistency_max_delay__", "1s"))
	require.NoError(t, consistency.Enforce(ctx))
	consistency.Consumed(0, 5, 100, time.Now().Add(-time.Minute).UnixMilli())
	require.Equal(t, codes.Unavailable, status.Code(consistency.Enforce(ctx)))
	consistency.Consumed(0, 99, 100, time.Now().Add(-time.Minute).UnixMilli())
	require.NoError(t, consistency.Enforce(ctx))
}

func TestConsumesSeeksAndCommitsAPartition(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, "topic"))
	require.NoError(t, err)
	t.Cleanup(cluster.Close)
	producer, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	require.NoError(t, err)
	t.Cleanup(producer.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	write := mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{{TimeSeries: &mimirpb.TimeSeries{
		Labels:  []mimirpb.LabelAdapter{{Name: "__name__", Value: "up"}},
		Samples: []mimirpb.Sample{{TimestampMs: 1, Value: 1}},
	}}}}
	payload, err := write.Marshal()
	require.NoError(t, err)
	base := time.UnixMilli(1_800_000_000_000)
	for index := range 5 {
		require.NoError(t, producer.ProduceSync(ctx, &kgo.Record{
			Topic: "topic", Partition: 1, Key: []byte("tenant"), Value: payload,
			Timestamp: base.Add(time.Duration(index) * time.Second),
			Headers:   []kgo.RecordHeader{{Key: "Version", Value: binary.BigEndian.AppendUint32(nil, 1)}},
		}).FirstErr())
	}

	client, err := Connect(Config{Brokers: cluster.ListenAddrs()[0], Topic: "topic", Partition: 1})
	require.NoError(t, err)
	t.Cleanup(client.Close)
	earliest, err := client.GetOffset(ctx, Earliest)
	require.NoError(t, err)
	latest, err := client.GetOffset(ctx, Latest)
	require.NoError(t, err)
	require.Equal(t, int64(0), earliest)
	require.Equal(t, int64(5), latest)
	offset, found, err := client.OffsetForTime(ctx, base.Add(2500*time.Millisecond).UnixMilli())
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(3), offset)
	_, found, err = client.OffsetForTime(ctx, base.Add(time.Hour).UnixMilli())
	require.NoError(t, err)
	require.False(t, found, "every record is older")

	require.NoError(t, client.Assign(1))
	for want := int64(1); want < 5; want++ {
		raw, highWatermark, err := client.NextRaw(ctx)
		require.NoError(t, err)
		require.Equal(t, want, raw.Offset)
		require.Equal(t, "tenant", raw.Tenant)
		require.Equal(t, uint32(1), raw.Version)
		require.Equal(t, int64(5), highWatermark)
		request, ok, err := raw.Decode()
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, [][2]string{{"__name__", "up"}}, request.Series[0].Labels)
	}
	// Seeking drops what was fetched.
	require.NoError(t, client.Assign(0))
	next, _, err := client.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), next.Offset)
	require.NoError(t, next.Record.Err)

	require.NoError(t, client.Commit(4))
	admin := kadm.NewClient(producer)
	require.Eventually(t, func() bool {
		committed, err := admin.FetchOffsets(ctx, ConsumerGroup)
		if err != nil {
			return false
		}
		at, ok := committed.Lookup("topic", 1)
		return ok && at.At == 4
	}, 10*time.Second, 50*time.Millisecond)
}
