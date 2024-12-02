package usagetracker

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPartitionIDFromInstanceID(t *testing.T) {
	t.Run("with zones", func(t *testing.T) {
		actual, err := partitionIDFromInstanceID("usage-tracker-zone-a-0")
		require.NoError(t, err)
		assert.EqualValues(t, 0, actual)

		actual, err = partitionIDFromInstanceID("usage-tracker-zone-b-0")
		require.NoError(t, err)
		assert.EqualValues(t, 0, actual)

		actual, err = partitionIDFromInstanceID("usage-tracker-zone-a-1")
		require.NoError(t, err)
		assert.EqualValues(t, 1, actual)

		actual, err = partitionIDFromInstanceID("usage-tracker-zone-b-1")
		require.NoError(t, err)
		assert.EqualValues(t, 1, actual)

		actual, err = partitionIDFromInstanceID("mimir-backend-zone-c-2")
		require.NoError(t, err)
		assert.EqualValues(t, 2, actual)
	})

	t.Run("without zones", func(t *testing.T) {
		actual, err := partitionIDFromInstanceID("usage-tracker-0")
		require.NoError(t, err)
		assert.EqualValues(t, 0, actual)

		actual, err = partitionIDFromInstanceID("usage-tracker-1")
		require.NoError(t, err)
		assert.EqualValues(t, 1, actual)

		actual, err = partitionIDFromInstanceID("mimir-backend-2")
		require.NoError(t, err)
		assert.EqualValues(t, 2, actual)
	})

	t.Run("should return error if the instance ID has a non supported format", func(t *testing.T) {
		_, err := partitionIDFromInstanceID("unknown")
		require.Error(t, err)

		_, err = partitionIDFromInstanceID("usage-tracker-zone-a-")
		require.Error(t, err)

		_, err = partitionIDFromInstanceID("usage-tracker-zone-a")
		require.Error(t, err)
	})
}
