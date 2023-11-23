package ingest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIngesterPartition(t *testing.T) {
	t.Run("should parse the ingester ID and return the partition ID", func(t *testing.T) {
		actual, err := IngesterPartition("ingester-zone-a-0")
		require.NoError(t, err)
		assert.Equal(t, 0, actual)

		actual, err = IngesterPartition("ingester-zone-b-0")
		require.NoError(t, err)
		assert.Equal(t, 1, actual)

		actual, err = IngesterPartition("ingester-zone-c-0")
		require.NoError(t, err)
		assert.Equal(t, 2, actual)

		actual, err = IngesterPartition("ingester-zone-a-1")
		require.NoError(t, err)
		assert.Equal(t, 4, actual)

		actual, err = IngesterPartition("ingester-zone-b-1")
		require.NoError(t, err)
		assert.Equal(t, 5, actual)

		actual, err = IngesterPartition("ingester-zone-c-1")
		require.NoError(t, err)
		assert.Equal(t, 6, actual)

		actual, err = IngesterPartition("ingester-zone-a-2")
		require.NoError(t, err)
		assert.Equal(t, 8, actual)

		actual, err = IngesterPartition("ingester-zone-b-2")
		require.NoError(t, err)
		assert.Equal(t, 9, actual)

		actual, err = IngesterPartition("ingester-zone-c-2")
		require.NoError(t, err)
		assert.Equal(t, 10, actual)
	})

	t.Run("should return error if the ingester ID has a non supported format", func(t *testing.T) {
		_, err := IngesterPartition("unknown")
		require.Error(t, err)

		_, err = IngesterPartition("ingester-zone-X-0")
		require.Error(t, err)

		_, err = IngesterPartition("ingester-zone-a--1")
		require.Error(t, err)
	})
}
