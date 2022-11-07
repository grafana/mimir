// SPDX-License-Identifier: AGPL-3.0-only

package indexheader

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCopyString(t *testing.T) {
	b := []byte("foo")
	s := yoloString(b)
	c := copyString(s)
	copy(b, "bar")
	require.Equal(t, "bar", s) // Still yoloed
	require.Equal(t, "foo", c) // Unaffected because copied
}
