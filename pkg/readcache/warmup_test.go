// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsStillWarming(t *testing.T) {
	t.Parallel()
	require.True(t, IsStillWarming(errStillWarming(7)))
	require.False(t, IsStillWarming(nil))
	require.False(t, IsStillWarming(errors.New("not a status")))
	require.False(t, IsStillWarming(status.Error(codes.Unavailable, "transport problem")))
	require.False(t, IsStillWarming(status.Error(codes.NotFound, stillWarmingDetail+" partition=1")))
}
