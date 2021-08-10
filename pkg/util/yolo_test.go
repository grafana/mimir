// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/yolo_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestYoloBuf(t *testing.T) {
	s := YoloBuf("hello world")

	require.Equal(t, []byte("hello world"), s)
}
