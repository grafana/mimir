package ingest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIngesterPartition(t *testing.T) {
	t.Run("with zones", func(t *testing.T) {
		actual, err := IngesterPartition("ingester-zone-a-0")
		require.NoError(t, err)
		assert.EqualValues(t, 0, actual)

		actual, err = IngesterPartition("ingester-zone-b-0")
		require.NoError(t, err)
		assert.EqualValues(t, 1, actual)

		actual, err = IngesterPartition("ingester-zone-c-0")
		require.NoError(t, err)
		assert.EqualValues(t, 2, actual)

		actual, err = IngesterPartition("ingester-zone-a-1")
		require.NoError(t, err)
		assert.EqualValues(t, 4, actual)

		actual, err = IngesterPartition("ingester-zone-b-1")
		require.NoError(t, err)
		assert.EqualValues(t, 5, actual)

		actual, err = IngesterPartition("ingester-zone-c-1")
		require.NoError(t, err)
		assert.EqualValues(t, 6, actual)

		actual, err = IngesterPartition("ingester-zone-a-2")
		require.NoError(t, err)
		assert.EqualValues(t, 8, actual)

		actual, err = IngesterPartition("ingester-zone-b-2")
		require.NoError(t, err)
		assert.EqualValues(t, 9, actual)

		actual, err = IngesterPartition("ingester-zone-c-2")
		require.NoError(t, err)
		assert.EqualValues(t, 10, actual)

		actual, err = IngesterPartition("mimir-write-zone-a-1")
		require.NoError(t, err)
		assert.EqualValues(t, 4, actual)

		actual, err = IngesterPartition("mimir-write-zone-b-1")
		require.NoError(t, err)
		assert.EqualValues(t, 5, actual)

		actual, err = IngesterPartition("mimir-write-zone-c-1")
		require.NoError(t, err)
		assert.EqualValues(t, 6, actual)
	})

	t.Run("without zones", func(t *testing.T) {
		actual, err := IngesterPartition("ingester-0")
		require.NoError(t, err)
		assert.EqualValues(t, 0, actual)

		actual, err = IngesterPartition("ingester-1")
		require.NoError(t, err)
		assert.EqualValues(t, 4, actual)

		actual, err = IngesterPartition("mimir-write-0")
		require.NoError(t, err)
		assert.EqualValues(t, 0, actual)

		actual, err = IngesterPartition("mimir-write-1")
		require.NoError(t, err)
		assert.EqualValues(t, 4, actual)
	})

	t.Run("should return error if the ingester ID has a non supported format", func(t *testing.T) {
		_, err := IngesterPartition("unknown")
		require.Error(t, err)

		_, err = IngesterPartition("ingester-zone-X-0")
		require.Error(t, err)

		_, err = IngesterPartition("ingester-zone-a-")
		require.Error(t, err)

		_, err = IngesterPartition("ingester-zone-a")
		require.Error(t, err)
	})
}

func TestKafkaClientID(t *testing.T) {
	assert.Equal(t, "", kafkaClientID("", ""))
	assert.Equal(t, "warpstream_proxy_target=proxy-produce", kafkaClientID("proxy-produce", ""))
	assert.Equal(t, "warpstream_proxy_target=proxy-produce,warpstream_az", kafkaClientID("proxy-produce", "us-east-2a"))
	assert.Equal(t, "warpstream_az=us-east-2a", kafkaClientID("", "us-east-2a"))
}
