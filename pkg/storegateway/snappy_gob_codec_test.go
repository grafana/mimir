// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/storegateway/indexcache"
)

func TestSnappyGobSeriesCacheEntryCodec(t *testing.T) {
	type testType struct {
		LabelSets   []labels.Labels
		MatchersKey indexcache.LabelMatchersKey
	}

	entry := testType{
		LabelSets: []labels.Labels{
			labels.FromStrings("foo", "bar"),
			labels.FromStrings("baz", "boo"),
		},
		MatchersKey: indexcache.CanonicalLabelMatchersKey([]*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "foo", "bar")}),
	}

	t.Run("happy case roundtrip", func(t *testing.T) {
		data, err := encodeSnappyGob(entry)
		require.NoError(t, err)

		var decoded testType
		err = decodeSnappyGob(data, &decoded)
		require.NoError(t, err)
		require.Equal(t, entry, decoded)
	})

	t.Run("can't decode wrong codec", func(t *testing.T) {
		data, err := encodeSnappyGob(entry)
		require.NoError(t, err)

		data[0] = 'x'

		var decoded testType
		err = decodeSnappyGob(data, &decoded)
		require.Error(t, err)
	})

	t.Run("can't decode wrong data", func(t *testing.T) {
		data, err := encodeSnappyGob(entry)
		require.NoError(t, err)

		data = data[:len(gobCodecPrefix)+1]

		var decoded testType
		err = decodeSnappyGob(data, &decoded)
		require.Error(t, err)
	})

	t.Run("encoding and decoding in different order", func(t *testing.T) {
		entry2 := testType{
			LabelSets: []labels.Labels{
				labels.FromStrings("a", "1"),
				labels.FromStrings("a", "2"),
			},
			MatchersKey: indexcache.CanonicalLabelMatchersKey([]*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "foo", "bar")}),
		}

		var (
			data [3][]byte
			err  error
		)
		data[0], err = encodeSnappyGob(entry)
		assert.NoError(t, err)
		data[1], err = encodeSnappyGob(entry2)
		assert.NoError(t, err)
		data[2], err = encodeSnappyGob(entry)
		assert.NoError(t, err)

		var decoded testType
		assert.NoError(t, decodeSnappyGob(data[1], &decoded))
		assert.Equal(t, entry2, decoded)

		assert.NoError(t, decodeSnappyGob(data[0], &decoded))
		assert.Equal(t, entry, decoded)

		assert.NoError(t, decodeSnappyGob(data[2], &decoded))
		assert.Equal(t, entry, decoded)
	})
}
