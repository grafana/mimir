// SPDX-License-Identifier: AGPL-3.0-only

package util

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCancellationError(t *testing.T) {
	directlyNestedErr := NewCancellationError(io.ErrNoProgress)
	require.True(t, errors.Is(directlyNestedErr, context.Canceled))
	require.True(t, errors.Is(directlyNestedErr, io.ErrNoProgress))
	require.False(t, errors.Is(directlyNestedErr, io.EOF))

	indirectlyNestedErr := NewCancellationError(fmt.Errorf("something went wrong: %w", io.ErrNoProgress))
	require.True(t, errors.Is(indirectlyNestedErr, context.Canceled))
	require.True(t, errors.Is(indirectlyNestedErr, io.ErrNoProgress))
	require.False(t, errors.Is(directlyNestedErr, io.EOF))
}
