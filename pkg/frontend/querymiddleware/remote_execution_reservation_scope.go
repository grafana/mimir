// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"errors"
	"sync"
)

var (
	errRemoteExecutionReservationScopeSealed = errors.New("remote execution group registered after the reservation scope was sealed")
	errRemoteExecutionReservationScopeClosed = errors.New("remote execution group registered after the reservation scope was closed")
)

// RemoteExecutionReservationScope collects the remote execution groups of one MQE evaluation on
// the query-frontend, so that the query-frontend can reserve query-scheduler queue slots for
// their sub-requests.
//
// Each group (RemoteExecutionGroup) of the evaluation calls Register in Prepare, and SealAndReserve in AfterPrepare,
// before it sends its sub-request. MQE calls AfterPrepare only after Prepare returned for all operators,
// so the first SealAndReserve counts every group of the evaluation.
type RemoteExecutionReservationScope struct {
	mu          sync.Mutex
	groups      int
	sealed      bool
	closed      bool
	reservation RemoteExecutionReservation
}

// RemoteExecutionReservation is the queue reservation of a sealed RemoteExecutionReservationScope.
type RemoteExecutionReservation interface {
	// Close tells the query-scheduler that the evaluation sends no more sub-requests, so that it
	// releases the idle slots. It aborts the reservation when aborted is true, and completes it
	// otherwise. It must not block.
	Close(aborted bool)
}

// RemoteExecutionReservationMember is the registration of one group, which Register returns.
type RemoteExecutionReservationMember struct {
	scope *RemoteExecutionReservationScope
}

func NewRemoteExecutionReservationScope() *RemoteExecutionReservationScope {
	return &RemoteExecutionReservationScope{}
}

// Register adds one group to the scope. It returns an error when the scope is already sealed or
// closed, because then the reserved count cannot include the group. MQE calls Prepare for all
// operators before AfterPrepare, so this error shows a bug.
func (s *RemoteExecutionReservationScope) Register() (*RemoteExecutionReservationMember, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, errRemoteExecutionReservationScopeClosed
	}
	if s.sealed {
		return nil, errRemoteExecutionReservationScopeSealed
	}

	s.groups++
	return &RemoteExecutionReservationMember{scope: s}, nil
}

// SealAndReserve stops the registration of groups and returns the reservation of the scope. The
// first call calls newReservation with the number of registered groups. Later calls return the
// same reservation. The reservation is nil when newReservation returns nil, or when the scope is
// closed. newReservation runs with the lock of the scope held, so it must not block.
func (m *RemoteExecutionReservationMember) SealAndReserve(newReservation func(groups int) RemoteExecutionReservation) RemoteExecutionReservation {
	s := m.scope
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.sealed {
		s.sealed = true
		if !s.closed {
			s.reservation = newReservation(s.groups)
		}
	}

	return s.reservation
}

// Close stops the scope, and closes its reservation if it has one. The evaluation calls it when
// it is done, with aborted true when the evaluation was cancelled.
func (s *RemoteExecutionReservationScope) Close(aborted bool) {
	s.mu.Lock()
	s.closed = true
	r := s.reservation
	s.mu.Unlock()

	if r != nil {
		r.Close(aborted)
	}
}

func ContextWithRemoteExecutionReservationScope(ctx context.Context, scope *RemoteExecutionReservationScope) context.Context {
	return context.WithValue(ctx, remoteExecutionReservationScopeKey, scope)
}

func RemoteExecutionReservationScopeFromContext(ctx context.Context) *RemoteExecutionReservationScope {
	if v := ctx.Value(remoteExecutionReservationScopeKey); v != nil {
		return v.(*RemoteExecutionReservationScope)
	}

	return nil
}

func ContextWithRemoteExecutionReservationMember(ctx context.Context, group *RemoteExecutionReservationMember) context.Context {
	return context.WithValue(ctx, remoteExecutionReservationMemberKey, group)
}

func RemoteExecutionReservationMemberFromContext(ctx context.Context) *RemoteExecutionReservationMember {
	if v := ctx.Value(remoteExecutionReservationMemberKey); v != nil {
		return v.(*RemoteExecutionReservationMember)
	}

	return nil
}
