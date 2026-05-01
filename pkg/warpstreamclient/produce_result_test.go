// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func makeTopicPartitionRecords(topic string, partition int32, payloads ...string) topicPartitionRecords {
	records := make([]*kgo.Record, len(payloads))
	for i, payload := range payloads {
		records[i] = &kgo.Record{Topic: topic, Partition: partition, Value: []byte(payload)}
	}
	return topicPartitionRecords{
		topic:     topic,
		partition: partition,
		records:   records,
	}
}

func partitionEntries(resp *kmsg.ProduceResponse, topic string) map[int32]kmsg.ProduceResponseTopicPartition {
	out := map[int32]kmsg.ProduceResponseTopicPartition{}
	for _, t := range resp.Topics {
		if t.Topic != topic {
			continue
		}
		for _, p := range t.Partitions {
			out[p.Partition] = p
		}
	}
	return out
}

// mustNewProduceResultAccumulator builds an accumulator and fails the
// test if the input has duplicate (topic, partition) pairs.
func mustNewProduceResultAccumulator(t *testing.T, partitions []topicPartitionRecords) *produceResultAccumulator {
	t.Helper()
	a, err := newProduceResultAccumulator(partitions)
	require.NoError(t, err)
	return a
}

func TestProduceResult_Error(t *testing.T) {
	tests := map[string]struct {
		res  produceResult
		want error
	}{
		"fully successful resp returns nil": {
			res: produceResult{resp: makeProduceResponse(0, 0, makeProduceResponseTopic("t",
				makeProduceResponseTopicPartition(0, kerrNoError),
			))},
			want: nil,
		},
		"transport err wins over resp": {
			res: produceResult{
				err: kerr.LeaderNotAvailable,
				resp: makeProduceResponse(0, 0, makeProduceResponseTopic("t",
					makeProduceResponseTopicPartition(0, kerrNoError),
				)),
			},
			want: kerr.LeaderNotAvailable,
		},
		"per-partition err: first non-zero ErrorCode": {
			res: produceResult{resp: makeProduceResponse(0, 0, makeProduceResponseTopic("t",
				makeProduceResponseTopicPartition(0, kerrNoError),
				makeProduceResponseTopicPartition(1, kerr.NotLeaderForPartition.Code),
			))},
			want: kerr.NotLeaderForPartition,
		},
		"empty produceResult (both nil) returns the empty sentinel": {
			res:  produceResult{},
			want: errEmptyProduceResult,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := tc.res.error()
			if tc.want == nil {
				assert.NoError(t, got)
			} else {
				require.Error(t, got)
				assert.ErrorIs(t, got, tc.want)
			}
		})
	}
}

func TestGetProduceResultErr(t *testing.T) {
	cases := map[string]struct {
		err  error
		want produceResultErr
	}{
		"context canceled": {
			err:  context.Canceled,
			want: produceResultErr{reason: "canceled", retriable: false, code: kerr.RequestTimedOut.Code},
		},
		"context deadline exceeded": {
			err:  context.DeadlineExceeded,
			want: produceResultErr{reason: "timeout", retriable: true, code: kerr.RequestTimedOut.Code},
		},
		"wrapped context canceled": {
			err:  fmt.Errorf("upstream: %w", context.Canceled),
			want: produceResultErr{reason: "canceled", retriable: false, code: kerr.RequestTimedOut.Code},
		},
		"wrapped context deadline exceeded": {
			err:  fmt.Errorf("attempt expired: %w", context.DeadlineExceeded),
			want: produceResultErr{reason: "timeout", retriable: true, code: kerr.RequestTimedOut.Code},
		},
		"retriable kerr error (LeaderNotAvailable)": {
			err:  kerr.LeaderNotAvailable,
			want: produceResultErr{reason: "kafka_retriable_error", retriable: true, code: kerr.LeaderNotAvailable.Code},
		},
		"retriable kerr error (RequestTimedOut)": {
			err:  kerr.RequestTimedOut,
			want: produceResultErr{reason: "kafka_retriable_error", retriable: true, code: kerr.RequestTimedOut.Code},
		},
		"wrapped retriable kerr error": {
			err:  fmt.Errorf("from broker: %w", kerr.NotLeaderForPartition),
			want: produceResultErr{reason: "kafka_retriable_error", retriable: true, code: kerr.NotLeaderForPartition.Code},
		},
		"transport error (io.EOF)": {
			err:  io.EOF,
			want: produceResultErr{reason: "transport", retriable: true, code: kerr.UnknownServerError.Code},
		},
		"non-retriable kerr error (MessageTooLarge)": {
			err:  kerr.MessageTooLarge,
			want: produceResultErr{reason: "kafka_non_retriable_error", retriable: false, code: kerr.MessageTooLarge.Code},
		},
		"non-retriable kerr error (TopicAuthorizationFailed)": {
			err:  kerr.TopicAuthorizationFailed,
			want: produceResultErr{reason: "kafka_non_retriable_error", retriable: false, code: kerr.TopicAuthorizationFailed.Code},
		},
		"wrapped non-retriable kerr error": {
			err:  fmt.Errorf("rejected: %w", kerr.MessageTooLarge),
			want: produceResultErr{reason: "kafka_non_retriable_error", retriable: false, code: kerr.MessageTooLarge.Code},
		},
		"unknown error": {
			err:  errors.New("something we don't recognise"),
			want: produceResultErr{reason: "unknown", retriable: false, code: kerr.UnknownServerError.Code},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			// getProduceResultErr stores the input err verbatim; fill in the
			// expectation here rather than repeating it in every case.
			tc.want.err = tc.err
			got := getProduceResultErr(tc.err)
			assert.Equal(t, tc.want, got)
			assert.NotZero(t, got.code)
		})
	}
}

func TestNewProduceResultAccumulator(t *testing.T) {
	t.Run("rejects duplicate topic-partition pairs", func(t *testing.T) {
		_, err := newProduceResultAccumulator([]topicPartitionRecords{
			makeTopicPartitionRecords("t", 0),
			makeTopicPartitionRecords("t", 1),
			makeTopicPartitionRecords("t", 0),
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), `topic="t"`)
		assert.Contains(t, err.Error(), "partition=0")
	})

	t.Run("same partition in different topics is not a duplicate", func(t *testing.T) {
		_, err := newProduceResultAccumulator([]topicPartitionRecords{
			makeTopicPartitionRecords("t", 0),
			makeTopicPartitionRecords("u", 0),
		})
		require.NoError(t, err)
	})

	t.Run("initial state: nothing resolved, all pending", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, []topicPartitionRecords{
			makeTopicPartitionRecords("t", 0),
			makeTopicPartitionRecords("t", 1),
		})
		d, err := a.done()
		assert.False(t, d)
		assert.NoError(t, err)
		assert.NotNil(t, a.result().err)
		assert.Len(t, a.remaining(), 2)
	})

	t.Run("empty input: nothing pending, done immediately", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, nil)
		d, err := a.done()
		assert.True(t, d)
		assert.NoError(t, err)
		r := a.result()
		assert.NoError(t, r.err)
		assert.Empty(t, r.resp.Topics)
	})
}

func TestProduceResultAccumulator_Accumulate(t *testing.T) {
	t.Run("full success: every partition resolved", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, []topicPartitionRecords{
			makeTopicPartitionRecords("t", 0),
			makeTopicPartitionRecords("t", 1),
		})

		a.accumulate(produceResult{resp: makeProduceResponse(9, 50, makeProduceResponseTopic("t",
			kmsg.ProduceResponseTopicPartition{Partition: 0, ErrorCode: kerrNoError, BaseOffset: 100},
			kmsg.ProduceResponseTopicPartition{Partition: 1, ErrorCode: kerrNoError, BaseOffset: 200},
		))})

		d, err := a.done()
		assert.True(t, d)
		assert.NoError(t, err)
		assert.Nil(t, a.result().err)
		assert.Empty(t, a.remaining())

		resp := a.result().resp
		assert.Equal(t, int16(9), resp.Version)
		assert.Equal(t, int32(50), resp.ThrottleMillis)
		entries := partitionEntries(resp, "t")
		require.Len(t, entries, 2)
		assert.Equal(t, kerrNoError, entries[0].ErrorCode)
		assert.Equal(t, int64(100), entries[0].BaseOffset)
		assert.Equal(t, kerrNoError, entries[1].ErrorCode)
		assert.Equal(t, int64(200), entries[1].BaseOffset)
	})

	t.Run("transport retriable err: not aborted, pending unchanged", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, []topicPartitionRecords{makeTopicPartitionRecords("t", 0)})

		a.accumulate(produceResult{err: kerr.LeaderNotAvailable})

		d, err := a.done()
		assert.False(t, d)
		require.Error(t, err)
		assert.ErrorIs(t, err, kerr.LeaderNotAvailable)
		assert.NotNil(t, a.result().err)
		assert.Len(t, a.remaining(), 1)
	})

	t.Run("transport non-retriable err: aborted", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, []topicPartitionRecords{makeTopicPartitionRecords("t", 0)})

		a.accumulate(produceResult{err: context.Canceled})

		d, err := a.done()
		assert.True(t, d)
		assert.ErrorIs(t, err, context.Canceled)
		assert.NotNil(t, a.result().err)

		resp := a.result().resp
		entries := partitionEntries(resp, "t")
		require.Len(t, entries, 1)
		// context.Canceled isn't a kerr.Error, so we fall back to
		// REQUEST_TIMED_OUT for the synthesized entry.
		assert.Equal(t, kerr.RequestTimedOut.Code, entries[0].ErrorCode)
	})

	t.Run("per-partition mixed retriable: whole leg treated as failed, partial successes dropped", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, []topicPartitionRecords{
			makeTopicPartitionRecords("t", 0),
			makeTopicPartitionRecords("t", 1),
		})

		a.accumulate(produceResult{resp: makeProduceResponse(0, 0, makeProduceResponseTopic("t",
			kmsg.ProduceResponseTopicPartition{Partition: 0, ErrorCode: kerrNoError, BaseOffset: 100},
			makeProduceResponseTopicPartition(1, kerr.NotLeaderForPartition.Code),
		))})

		d, err := a.done()
		assert.False(t, d)
		assert.ErrorIs(t, err, kerr.NotLeaderForPartition)
		assert.NotNil(t, a.result().err)
		assert.Len(t, a.remaining(), 2)
	})

	t.Run("per-partition mixed non-retriable: aborted and pending synthesized with the aborted kerr code", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, []topicPartitionRecords{
			makeTopicPartitionRecords("t", 0),
			makeTopicPartitionRecords("t", 1),
		})

		a.accumulate(produceResult{resp: makeProduceResponse(0, 0, makeProduceResponseTopic("t",
			kmsg.ProduceResponseTopicPartition{Partition: 0, ErrorCode: kerrNoError, BaseOffset: 100},
			makeProduceResponseTopicPartition(1, kerr.MessageTooLarge.Code),
		))})

		d, err := a.done()
		assert.True(t, d)
		assert.ErrorIs(t, err, kerr.MessageTooLarge)
		assert.NotNil(t, a.result().err)

		resp := a.result().resp
		entries := partitionEntries(resp, "t")
		require.Len(t, entries, 2)
		assert.Equal(t, kerr.MessageTooLarge.Code, entries[0].ErrorCode)
		assert.Equal(t, kerr.MessageTooLarge.Code, entries[1].ErrorCode)
	})

	t.Run("partial response coverage: only the included partitions resolve", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, []topicPartitionRecords{
			makeTopicPartitionRecords("t", 0),
			makeTopicPartitionRecords("t", 1),
		})

		a.accumulate(produceResult{resp: makeProduceResponse(0, 0, makeProduceResponseTopic("t",
			kmsg.ProduceResponseTopicPartition{Partition: 0, ErrorCode: kerrNoError, BaseOffset: 100},
		))})

		d, _ := a.done()
		assert.False(t, d)
		// One partition resolved but the other is still pending: result
		// surfaces bare ErrRecordTimeout (no failure was observed).
		assert.ErrorIs(t, a.result().err, kgo.ErrRecordTimeout)
		rem := a.remaining()
		require.Len(t, rem, 1)
		assert.Equal(t, int32(1), rem[0].partition)
	})

	t.Run("multiple successful calls together resolve everything and capture metadata", func(t *testing.T) {
		tid := [16]byte{1, 2, 3}
		a := mustNewProduceResultAccumulator(t, []topicPartitionRecords{
			makeTopicPartitionRecords("t", 0),
			makeTopicPartitionRecords("t", 1),
		})

		first := makeProduceResponse(9, 100, makeProduceResponseTopic("t",
			kmsg.ProduceResponseTopicPartition{Partition: 0, ErrorCode: kerrNoError, BaseOffset: 100},
		))
		first.Topics[0].TopicID = tid

		second := makeProduceResponse(11, 50, makeProduceResponseTopic("t",
			kmsg.ProduceResponseTopicPartition{Partition: 1, ErrorCode: kerrNoError, BaseOffset: 200},
		))

		a.accumulate(produceResult{resp: first})
		d, _ := a.done()
		assert.False(t, d)
		// Intermediate state: one partition resolved but one still
		// pending and no error observed → bare ErrRecordTimeout.
		assert.ErrorIs(t, a.result().err, kgo.ErrRecordTimeout)

		a.accumulate(produceResult{resp: second})
		d, _ = a.done()
		assert.True(t, d)
		assert.Nil(t, a.result().err)

		resp := a.result().resp
		assert.Equal(t, int16(9), resp.Version, "version is from the first response")
		assert.Equal(t, int32(100), resp.ThrottleMillis, "throttle is the max across responses")
		require.Len(t, resp.Topics, 1)
		assert.Equal(t, tid, resp.Topics[0].TopicID)
		entries := partitionEntries(resp, "t")
		require.Len(t, entries, 2)
		assert.Equal(t, int64(100), entries[0].BaseOffset)
		assert.Equal(t, int64(200), entries[1].BaseOffset)
	})

	t.Run("overwrites already-resolved entry on later accumulate", func(t *testing.T) {
		// Real callers don't re-submit a resolved partition (claimNextWave
		// skips it), so this case shouldn't fire in practice. If it ever
		// does — e.g. a buggy agent echoing a partition we didn't ask
		// about, or a future caller mistake — the last accumulate wins.
		a := mustNewProduceResultAccumulator(t, []topicPartitionRecords{makeTopicPartitionRecords("t", 0)})

		a.accumulate(produceResult{resp: makeProduceResponse(0, 0, makeProduceResponseTopic("t",
			kmsg.ProduceResponseTopicPartition{Partition: 0, ErrorCode: kerrNoError, BaseOffset: 1},
		))})
		d, _ := a.done()
		require.True(t, d)

		a.accumulate(produceResult{resp: makeProduceResponse(0, 0, makeProduceResponseTopic("t",
			kmsg.ProduceResponseTopicPartition{Partition: 0, ErrorCode: kerrNoError, BaseOffset: 999},
		))})

		resp := a.result().resp
		entries := partitionEntries(resp, "t")
		require.Len(t, entries, 1)
		assert.Equal(t, int64(999), entries[0].BaseOffset, "last accumulate wins for an already-resolved partition")
	})
}

func TestProduceResultAccumulator_Response(t *testing.T) {
	t.Run("exhausted retriable: pending entries inherit the leg's kerr code", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, []topicPartitionRecords{
			makeTopicPartitionRecords("t", 0),
			makeTopicPartitionRecords("u", 7),
		})

		a.accumulate(produceResult{err: kerr.LeaderNotAvailable})

		resp := a.result().resp
		require.Len(t, resp.Topics, 2)
		for _, topic := range resp.Topics {
			require.Len(t, topic.Partitions, 1)
			assert.Equal(t, kerr.LeaderNotAvailable.Code, topic.Partitions[0].ErrorCode)
		}
	})

	t.Run("aborted on non-kerr err: pending entries fall back to UNKNOWN_SERVER_ERROR", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, []topicPartitionRecords{makeTopicPartitionRecords("t", 0)})

		a.accumulate(produceResult{err: errors.New("some non-retriable transport boom")})

		resp := a.result().resp
		entries := partitionEntries(resp, "t")
		require.Len(t, entries, 1)
		assert.Equal(t, kerr.UnknownServerError.Code, entries[0].ErrorCode)
	})

	t.Run("pending partition with recorded per-partition failure surfaces that entry", func(t *testing.T) {
		// One mixed-failure leg; the partition that failed retriably
		// has its actual entry recorded so response() can surface it
		// instead of a synthesized REQUEST_TIMED_OUT.
		a := mustNewProduceResultAccumulator(t, []topicPartitionRecords{
			makeTopicPartitionRecords("t", 0),
			makeTopicPartitionRecords("t", 1),
		})

		a.accumulate(produceResult{resp: makeProduceResponse(0, 0, makeProduceResponseTopic("t",
			kmsg.ProduceResponseTopicPartition{Partition: 0, ErrorCode: kerrNoError, BaseOffset: 100},
			makeProduceResponseTopicPartition(1, kerr.NotLeaderForPartition.Code),
		))})

		resp := a.result().resp
		entries := partitionEntries(resp, "t")
		require.Len(t, entries, 2)
		// Partition 0 had no recorded failure entry (its success was
		// dropped under all-or-nothing), so it inherits the leg's
		// kerr code from lastErr.
		assert.Equal(t, kerr.NotLeaderForPartition.Code, entries[0].ErrorCode)
		// Partition 1 surfaces the actual error reported by the agent.
		assert.Equal(t, kerr.NotLeaderForPartition.Code, entries[1].ErrorCode)
	})

	t.Run("failed entry is cleared once the partition later resolves", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, []topicPartitionRecords{makeTopicPartitionRecords("t", 0)})

		// First leg: partition fails retriably (recorded in failed map).
		a.accumulate(produceResult{resp: makeProduceResponse(0, 0, makeProduceResponseTopic("t",
			makeProduceResponseTopicPartition(0, kerr.NotLeaderForPartition.Code),
		))})
		// Second leg: partition succeeds — should drop the failed entry.
		a.accumulate(produceResult{resp: makeProduceResponse(0, 0, makeProduceResponseTopic("t",
			kmsg.ProduceResponseTopicPartition{Partition: 0, ErrorCode: kerrNoError, BaseOffset: 42},
		))})

		resp := a.result().resp
		entries := partitionEntries(resp, "t")
		require.Len(t, entries, 1)
		assert.Equal(t, kerrNoError, entries[0].ErrorCode)
		assert.Equal(t, int64(42), entries[0].BaseOffset)
	})

	t.Run("partial resolved plus pending: resolved entry preserved, pending synthesized", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, []topicPartitionRecords{
			makeTopicPartitionRecords("t", 0),
			makeTopicPartitionRecords("t", 1),
		})

		a.accumulate(produceResult{resp: makeProduceResponse(9, 0, makeProduceResponseTopic("t",
			kmsg.ProduceResponseTopicPartition{Partition: 0, ErrorCode: kerrNoError, BaseOffset: 42},
		))})
		a.accumulate(produceResult{err: kerr.LeaderNotAvailable})

		resp := a.result().resp
		entries := partitionEntries(resp, "t")
		require.Len(t, entries, 2)
		assert.Equal(t, kerrNoError, entries[0].ErrorCode)
		assert.Equal(t, int64(42), entries[0].BaseOffset)
		// The unresolved partition inherits the most recent leg's kerr
		// code so perPartitionDone can surface the specific failure.
		assert.Equal(t, kerr.LeaderNotAvailable.Code, entries[1].ErrorCode)
	})
}

func TestProduceResultAccumulator_Result(t *testing.T) {
	t.Run("all partitions resolved: no err, response carries every entry", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, []topicPartitionRecords{
			makeTopicPartitionRecords("t", 0),
			makeTopicPartitionRecords("t", 1),
		})
		a.accumulate(produceResult{resp: makeProduceResponse(9, 100, makeProduceResponseTopic("t",
			kmsg.ProduceResponseTopicPartition{Partition: 0, ErrorCode: kerrNoError, BaseOffset: 42},
			kmsg.ProduceResponseTopicPartition{Partition: 1, ErrorCode: kerrNoError, BaseOffset: 43},
		))})

		r := a.result()
		require.NoError(t, r.err)
		require.NotNil(t, r.resp)
		entries := partitionEntries(r.resp, "t")
		require.Len(t, entries, 2)
		assert.Equal(t, int64(42), entries[0].BaseOffset)
		assert.Equal(t, int64(43), entries[1].BaseOffset)
	})

	t.Run("partially resolved, no err observed: bare ErrRecordTimeout, pending entries synthesized in response", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, []topicPartitionRecords{
			makeTopicPartitionRecords("t", 0),
			makeTopicPartitionRecords("t", 1),
		})
		a.accumulate(produceResult{resp: makeProduceResponse(9, 0, makeProduceResponseTopic("t",
			kmsg.ProduceResponseTopicPartition{Partition: 0, ErrorCode: kerrNoError, BaseOffset: 42},
		))})

		r := a.result()
		require.Error(t, r.err)
		assert.ErrorIs(t, r.err, kgo.ErrRecordTimeout)
		entries := partitionEntries(r.resp, "t")
		require.Len(t, entries, 2)
		assert.Equal(t, kerrNoError, entries[0].ErrorCode)
		assert.Equal(t, kerr.RequestTimedOut.Code, entries[1].ErrorCode)
	})

	t.Run("partially resolved with failed leg: lastErr wins, wrapped in ErrRecordTimeout", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, []topicPartitionRecords{
			makeTopicPartitionRecords("t", 0),
			makeTopicPartitionRecords("t", 1),
		})
		// First leg resolves partition 0; second leg's transport error
		// leaves partition 1 pending and is recorded as lastErr.
		a.accumulate(produceResult{resp: makeProduceResponse(9, 0, makeProduceResponseTopic("t",
			kmsg.ProduceResponseTopicPartition{Partition: 0, ErrorCode: kerrNoError, BaseOffset: 42},
		))})
		a.accumulate(produceResult{err: kerr.LeaderNotAvailable})

		r := a.result()
		require.Error(t, r.err)
		assert.ErrorIs(t, r.err, kgo.ErrRecordTimeout)
		assert.ErrorIs(t, r.err, kerr.LeaderNotAvailable)
	})

	t.Run("nothing resolved, no err observed: bare kgo.ErrRecordTimeout", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, []topicPartitionRecords{makeTopicPartitionRecords("t", 0)})

		r := a.result()
		require.Error(t, r.err)
		assert.ErrorIs(t, r.err, kgo.ErrRecordTimeout)
		assert.NotErrorIs(t, r.err, kerr.LeaderNotAvailable)
	})

	t.Run("nothing resolved, retriable err observed: kgo.ErrRecordTimeout wraps the kerr", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, []topicPartitionRecords{makeTopicPartitionRecords("t", 0)})
		a.accumulate(produceResult{err: kerr.LeaderNotAvailable})

		r := a.result()
		require.Error(t, r.err)
		assert.ErrorIs(t, r.err, kgo.ErrRecordTimeout)
		assert.ErrorIs(t, r.err, kerr.LeaderNotAvailable)
	})

	t.Run("nothing resolved, non-retriable err observed: kgo.ErrRecordTimeout wraps the kerr", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, []topicPartitionRecords{makeTopicPartitionRecords("t", 0)})
		a.accumulate(produceResult{err: kerr.MessageTooLarge})

		r := a.result()
		require.Error(t, r.err)
		assert.ErrorIs(t, r.err, kgo.ErrRecordTimeout)
		assert.ErrorIs(t, r.err, kerr.MessageTooLarge)
	})

	t.Run("empty accumulator: no err (vacuously resolved), empty response", func(t *testing.T) {
		a := mustNewProduceResultAccumulator(t, nil)

		r := a.result()
		assert.NoError(t, r.err)
		require.NotNil(t, r.resp)
		assert.Empty(t, r.resp.Topics)
	})
}

func TestSelectProduceResult(t *testing.T) {
	successResp := &kmsg.ProduceResponse{Topics: []kmsg.ProduceResponseTopic{{
		Topic:      "t",
		Partitions: []kmsg.ProduceResponseTopicPartition{{Partition: 0, ErrorCode: 0, BaseOffset: 42}},
	}}}
	primaryCodesResp := &kmsg.ProduceResponse{Topics: []kmsg.ProduceResponseTopic{{
		Topic:      "t",
		Partitions: []kmsg.ProduceResponseTopicPartition{{Partition: 0, ErrorCode: kerr.NotEnoughReplicas.Code}},
	}}}
	fallbackCodesResp := &kmsg.ProduceResponse{Topics: []kmsg.ProduceResponseTopic{{
		Topic:      "t",
		Partitions: []kmsg.ProduceResponseTopicPartition{{Partition: 0, ErrorCode: kerr.NotLeaderForPartition.Code}},
	}}}
	primaryErr := kerr.LeaderNotAvailable
	fallbackErr := fmt.Errorf("%w: %w", kgo.ErrRecordTimeout, kerr.NotLeaderForPartition)

	tests := map[string]struct {
		primary  produceResult
		fallback produceResult
		want     produceResult
	}{
		"primary succeeded: return primary, ignore fallback": {
			primary:  produceResult{resp: successResp},
			fallback: produceResult{resp: nil, err: fallbackErr},
			want:     produceResult{resp: successResp},
		},
		"primary errored, fallback succeeded: return fallback": {
			primary:  produceResult{err: primaryErr},
			fallback: produceResult{resp: successResp},
			want:     produceResult{resp: successResp},
		},
		"primary errored, fallback errored: fallback.resp (merged view) + chained primary.err: fallback.err": {
			primary:  produceResult{err: primaryErr},
			fallback: produceResult{resp: fallbackCodesResp, err: fallbackErr},
			want: produceResult{
				resp: fallbackCodesResp,
				err:  fmt.Errorf("%w: %w", primaryErr, fallbackErr),
			},
		},
		"primary had per-partition codes only (no transport err), fallback errored: return primary": {
			primary:  produceResult{resp: primaryCodesResp},
			fallback: produceResult{resp: fallbackCodesResp, err: fallbackErr},
			want:     produceResult{resp: primaryCodesResp},
		},
		"primary transport-errored, fallback had per-partition codes only (partial wins): return fallback": {
			primary:  produceResult{err: primaryErr},
			fallback: produceResult{resp: fallbackCodesResp},
			want:     produceResult{resp: fallbackCodesResp},
		},
		"both had per-partition codes only (no transport errs): return fallback (its view is the latest)": {
			primary:  produceResult{resp: primaryCodesResp},
			fallback: produceResult{resp: fallbackCodesResp},
			want:     produceResult{resp: fallbackCodesResp},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := selectProduceResult(tc.primary, tc.fallback)
			assert.Same(t, tc.want.resp, got.resp)
			if tc.want.err == nil {
				assert.NoError(t, got.err)
			} else {
				require.Error(t, got.err)
				assert.Equal(t, tc.want.err.Error(), got.err.Error())
			}
		})
	}
}
