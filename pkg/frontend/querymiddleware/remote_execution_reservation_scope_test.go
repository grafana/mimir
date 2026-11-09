// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type closeCountingReservation struct {
	closes  int
	aborted bool
}

func (r *closeCountingReservation) Close(aborted bool) {
	r.closes++
	r.aborted = aborted
}

func TestRemoteExecutionReservationScope_SealAndReserveCountsRegisteredGroups(t *testing.T) {
	for name, aborted := range map[string]bool{"completed": false, "aborted": true} {
		t.Run(name, func(t *testing.T) {
			testRemoteExecutionReservationScopeSealAndReserve(t, aborted)
		})
	}
}

func testRemoteExecutionReservationScopeSealAndReserve(t *testing.T, aborted bool) {
	scope := NewRemoteExecutionReservationScope()
	first, err := scope.Register()
	require.NoError(t, err)
	second, err := scope.Register()
	require.NoError(t, err)

	reservation := &closeCountingReservation{}
	calls := 0
	newReservation := func(groups int) RemoteExecutionReservation {
		calls++
		require.Equal(t, 2, groups)
		return reservation
	}

	require.Same(t, reservation, first.SealAndReserve(newReservation))
	require.Same(t, reservation, second.SealAndReserve(newReservation), "a later SealAndReserve returns the same reservation")
	require.Equal(t, 1, calls)
	_, err = scope.Register()
	require.ErrorIs(t, err, errRemoteExecutionReservationScopeSealed)

	scope.Close(aborted)
	require.Equal(t, 1, reservation.closes)
	require.Equal(t, aborted, reservation.aborted)
}

func TestRemoteExecutionReservationScope_SealAndReserveAfterClose(t *testing.T) {
	scope := NewRemoteExecutionReservationScope()
	group, err := scope.Register()
	require.NoError(t, err)
	scope.Close(false)

	_, err = scope.Register()
	require.ErrorIs(t, err, errRemoteExecutionReservationScopeClosed)
	require.Nil(t, group.SealAndReserve(func(int) RemoteExecutionReservation {
		require.FailNow(t, "a closed scope must not create a reservation")
		return nil
	}))
}
