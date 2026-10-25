// SPDX-License-Identifier: AGPL-3.0-only

// Command kafkafetch dumps partitions' last records, a JSON record per line as kafkatool's dump export writes them, for
// the native metadata prototype's samples of a cell's records. It fetches as Mimir's ingesters do: Fetch v13 requests
// sent to the partition's leader, without fetch sessions or a consumer group, so it commits nothing. A partition's
// sample is the records before the end offset it had when the sample started, and kafkafetch fails unless it has all
// of them within the byte and time limits. Its client ID needs the ingesters' routing settings, which are what lets
// the fetches reach the partitions. SASL credentials come from the U and P environment variables, if set.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

func main() {
	address := flag.String("kafka-address", "", "Kafka broker address.")
	clientID := flag.String("kafka-client-id", "", "Kafka client ID.")
	topic := flag.String("topic", "ingest", "Topic.")
	partitions := flag.String("partitions", "0", "Partitions to dump, comma separated, or active:N for N spread over the ones that took records in 10 seconds.")
	records := flag.Int64("records", 2000, "Records before each partition's end to dump.")
	maxBytes := flag.Int64("max-bytes", 6_000_000_000, "Stop a partition's dump once it has this many bytes.")
	prefix := flag.String("file-prefix", "/dump-", "Output files, <prefix><partition>.json, a JSON record per line.")
	timeout := flag.Duration("timeout", 10*time.Minute, "Fail unless every partition is dumped within this long.")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	client, err := newClient(*address, *clientID)
	if err != nil {
		fail(err)
	}
	defer client.Close()
	chosen, err := choose(ctx, client, *topic, *partitions)
	if err != nil {
		fail(err)
	}
	fmt.Fprintf(os.Stderr, "partitions %v\n", chosen)
	for _, p := range chosen {
		if err := run(ctx, client, *topic, p, *records, *maxBytes, fmt.Sprintf("%s%d.json", *prefix, p)); err != nil {
			fail(err)
		}
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

func newClient(address, clientID string) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(address),
		kgo.ClientID(clientID),
		kgo.MaxVersions(kversion.V3_9_0()),
		kgo.BrokerMaxReadBytes(1_000_000_000),
	}
	if user := os.Getenv("U"); user != "" {
		opts = append(opts, kgo.SASL(plain.Auth{User: user, Pass: os.Getenv("P")}.AsMechanism()))
	}
	return kgo.NewClient(opts...)
}

// choose is the partitions to dump: those listed, or active:N, N spread evenly over the partitions whose end moved in
// 10 seconds.
func choose(ctx context.Context, client *kgo.Client, topic, spec string) ([]int32, error) {
	if n, ok := strings.CutPrefix(spec, "active:"); ok {
		want, err := strconv.Atoi(n)
		if err != nil {
			return nil, err
		}
		adm := kadm.NewClient(client)
		before, err := adm.ListEndOffsets(ctx, topic)
		if err != nil {
			return nil, err
		}
		time.Sleep(10 * time.Second)
		after, err := adm.ListEndOffsets(ctx, topic)
		if err != nil {
			return nil, err
		}
		var active []int32
		after.Each(func(o kadm.ListedOffset) {
			if b, ok := before.Lookup(topic, o.Partition); ok && o.Err == nil && o.Offset > b.Offset {
				active = append(active, o.Partition)
			}
		})
		slices.Sort(active)
		fmt.Fprintf(os.Stderr, "%d partitions active\n", len(active))
		var out []int32
		for i := range min(want, len(active)) {
			out = append(out, active[i*len(active)/min(want, len(active))])
		}
		return out, nil
	}
	var out []int32
	for _, f := range strings.Split(spec, ",") {
		p, err := strconv.Atoi(f)
		if err != nil {
			return nil, err
		}
		out = append(out, int32(p))
	}
	return out, nil
}

type countingWriter struct {
	f *os.File
	n int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.f.Write(p)
	w.n += int64(n)
	return n, err
}

// run dumps the partition's records from records before its end offset to it, into file, and fails unless it has
// them all.
func run(ctx context.Context, client *kgo.Client, topic string, partition int32, records, maxBytes int64, file string) error {
	adm := kadm.NewClient(client)

	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return fmt.Errorf("listing end offsets: %w", err)
	}
	listed, ok := ends.Lookup(topic, partition)
	if !ok || listed.Err != nil {
		return fmt.Errorf("end offset of partition %d: found %t, %v", partition, ok, listed.Err)
	}
	end := listed.Offset
	start := max(end-records, 0)
	offset := start
	fmt.Fprintf(os.Stderr, "partition %d: %d to %d\n", partition, start, end)

	f, err := os.Create(file)
	if err != nil {
		return err
	}
	defer f.Close()
	out := &countingWriter{f: f}
	encoder := json.NewEncoder(out)

	dumped, empty := 0, 0
	for offset < end && out.n < maxBytes {
		metadata, err := adm.Metadata(ctx, topic)
		if err != nil {
			return fmt.Errorf("metadata: %w", err)
		}
		details := metadata.Topics[topic]
		leader := details.Partitions[partition]

		req := kmsg.NewFetchRequest()
		req.Version = 13
		req.MinBytes = 1
		req.MaxWaitMillis = 5000
		req.MaxBytes = 100_000_000
		reqTopic := kmsg.NewFetchRequestTopic()
		reqTopic.Topic = topic
		reqTopic.TopicID = details.ID
		reqPartition := kmsg.NewFetchRequestTopicPartition()
		reqPartition.Partition = partition
		reqPartition.FetchOffset = offset
		reqPartition.PartitionMaxBytes = 50_000_000
		reqPartition.CurrentLeaderEpoch = leader.LeaderEpoch
		reqTopic.Partitions = append(reqTopic.Partitions, reqPartition)
		req.Topics = append(req.Topics, reqTopic)

		resp, err := req.RequestWith(ctx, client.Broker(int(leader.Leader)))
		if err != nil {
			return fmt.Errorf("fetch from broker %d: %w", leader.Leader, err)
		}
		if resp.ErrorCode != 0 || len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
			return fmt.Errorf("unexpected fetch response: error code %d, %d topics", resp.ErrorCode, len(resp.Topics))
		}
		fetched, _ := kgo.ProcessFetchPartition(kgo.ProcessFetchPartitionOpts{
			Offset:         offset,
			IsolationLevel: kgo.ReadUncommitted(),
			Topic:          topic,
			Partition:      partition,
		}, &resp.Topics[0].Partitions[0], kgo.DefaultDecompressor(), nil)
		if fetched.Err != nil {
			return fmt.Errorf("fetch at offset %d: %w", offset, fetched.Err)
		}
		if len(fetched.Records) == 0 {
			if empty++; empty == 12 {
				return fmt.Errorf("no records at offset %d after %d fetches", offset, empty)
			}
			continue
		}
		empty = 0
		for _, r := range fetched.Records {
			if r.Offset >= end {
				// None left before the end, as the partition's later records start past it.
				offset = end
				break
			}
			if out.n >= maxBytes {
				break
			}
			if err := encoder.Encode(r); err != nil {
				return err
			}
			offset = r.Offset + 1
			dumped++
		}
		fmt.Fprintf(os.Stderr, "dumped %d records, %d bytes, at offset %d\n", dumped, out.n, offset)
	}
	if offset < end {
		return fmt.Errorf("partition %d: incomplete, the byte limit stopped it at offset %d of [%d, %d), with %d records, %d bytes", partition, offset, start, end, dumped, out.n)
	}
	fmt.Fprintf(os.Stderr, "partition %d: complete, offsets [%d, %d), %d records, %d bytes\n", partition, start, end, dumped, out.n)
	return nil
}
