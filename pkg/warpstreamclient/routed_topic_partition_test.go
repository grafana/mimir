// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestNewMultiRoutedTopicPartitionRecords(t *testing.T) {
	t.Run("stamps every entry with the same nodeID and done", func(t *testing.T) {
		parts := []topicPartitionRecords{
			{topic: "t", partition: 0, records: []*kgo.Record{{Topic: "t", Partition: 0}}},
			{topic: "t", partition: 1, records: []*kgo.Record{{Topic: "t", Partition: 1}}},
			{topic: "u", partition: 5, records: []*kgo.Record{{Topic: "u", Partition: 5}}},
		}
		var fired int
		done := func(ProduceResult) { fired++ }

		out := newMultiRoutedTopicPartitionRecords(parts, 42, done)
		require.Len(t, out, len(parts))
		for i, r := range out {
			assert.Equal(t, parts[i].topic, r.topic)
			assert.Equal(t, parts[i].partition, r.partition)
			assert.Equal(t, parts[i].records, r.records)
			assert.Equal(t, int32(42), r.nodeID)
			require.NotNil(t, r.done)
			r.done(ProduceResult{})
		}
		assert.Equal(t, len(parts), fired)
	})

	t.Run("empty input returns empty slice", func(t *testing.T) {
		out := newMultiRoutedTopicPartitionRecords(nil, 1, func(ProduceResult) {})
		assert.Empty(t, out)
	})

	t.Run("nil done is preserved", func(t *testing.T) {
		parts := []topicPartitionRecords{{topic: "t", partition: 0}}
		out := newMultiRoutedTopicPartitionRecords(parts, 1, nil)
		require.Len(t, out, 1)
		assert.Nil(t, out[0].done)
	})

	t.Run("done propagates the ProduceResult to every entry", func(t *testing.T) {
		parts := []topicPartitionRecords{
			{topic: "t", partition: 0},
			{topic: "t", partition: 1},
		}
		want := errors.New("boom")
		resp := &kmsg.ProduceResponse{}
		var calls []ProduceResult
		done := func(res ProduceResult) {
			calls = append(calls, res)
		}

		out := newMultiRoutedTopicPartitionRecords(parts, 7, done)
		for _, r := range out {
			r.done(ProduceResult{resp: resp, err: want})
		}
		require.Len(t, calls, len(parts))
		for _, c := range calls {
			assert.Same(t, resp, c.resp)
			assert.Same(t, want, c.err)
		}
	})
}
