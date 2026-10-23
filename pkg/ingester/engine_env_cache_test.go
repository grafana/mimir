// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// The engine under test is read before go test records the environment a run reads, which it caches results by.
// Reading it again while the tests run makes a run on another engine miss the cache, rather than reuse the result of
// the last run on the default one.
func TestEngineUnderTestKeysTheTestCache(t *testing.T) {
	require.Equal(t, os.Getenv("MIMIR_TEST_TSDB_ENGINE"), testEngine)
	require.Equal(t, os.Getenv("MIMIR_TEST_NO_NATIVE_ACTIVE") != "", testNoNativeActive)
}
