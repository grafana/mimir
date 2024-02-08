package ring

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"go.uber.org/atomic"

	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/services"
)

var (
	ErrPartitionDoesNotExist          = errors.New("the partition does not exist")
	ErrPartitionStateMismatch         = errors.New("the partition state does not match the expected one")
	ErrPartitionStateChangeNotAllowed = errors.New("partition state change not allowed")

	allowedPartitionStateChanges = map[PartitionState][]PartitionState{
		PartitionPending:  {PartitionActive, PartitionInactive},
		PartitionActive:   {PartitionInactive},
		PartitionInactive: {PartitionPending, PartitionActive},
	}
)

type PartitionInstanceLifecyclerConfig struct {
	// PartitionID is the ID of the partition managed by the lifecycler.
	PartitionID int32

	// InstanceID is the ID of the instance managed by the lifecycler.
	InstanceID string

	// WaitOwnersCountOnPending is the minimum number of owners to wait before switching a
	// PENDING partition to ACTIVE.
	WaitOwnersCountOnPending int

	// WaitOwnersDurationOnPending is how long each owner should have been added to the
	// partition before it's considered eligible for the WaitOwnersCountOnPending count.
	WaitOwnersDurationOnPending time.Duration

	// DeleteInactivePartitionAfterDuration is how long the lifecycler should wait before
	// deleting inactive partitions. Inactive partitions are never removed if this value is 0.
	//
	// - The owned partition is deleted only when stopping the lifecycler and if it has been
	//   in inactive state for longer than DeleteInactivePartitionAfterDuration.
	// - The other partitions are deleted as part of a periodic reconciliation done by the
	//   lifecycler if they've been in inactive state for longer than 2x DeleteInactivePartitionAfterDuration.
	DeleteInactivePartitionAfterDuration time.Duration

	reconcileInterval time.Duration
}

// PartitionInstanceLifecycler is responsible to manage the lifecycle of a single
// partition and partition owner in the ring.
type PartitionInstanceLifecycler struct {
	*services.BasicService

	// These values are initialised at startup, and never change.
	cfg      PartitionInstanceLifecyclerConfig
	ringName string
	ringKey  string
	store    kv.Client
	logger   log.Logger

	// Channel used to execute logic within the lifecycler loop.
	actorChan chan func()

	// Whether the lifecycler should remove the partition owner on shutdown.
	removeOwnerOnShutdown *atomic.Bool
}

func NewPartitionInstanceLifecycler(cfg PartitionInstanceLifecyclerConfig, ringName, ringKey string, store kv.Client, logger log.Logger) *PartitionInstanceLifecycler {
	if cfg.reconcileInterval == 0 {
		cfg.reconcileInterval = 5 * time.Second
	}

	l := &PartitionInstanceLifecycler{
		cfg:                   cfg,
		ringName:              ringName,
		ringKey:               ringKey,
		store:                 store,
		logger:                log.With(logger, "ring", ringName, "partition", cfg.PartitionID),
		actorChan:             make(chan func()),
		removeOwnerOnShutdown: atomic.NewBool(false),
	}

	l.BasicService = services.NewBasicService(l.starting, l.running, l.stopping)

	return l
}

// RemoveOwnerOnShutdown returns whether the lifecycler has been configured to remove the partition
// owner on shutdown.
func (l *PartitionInstanceLifecycler) RemoveOwnerOnShutdown() bool {
	return l.removeOwnerOnShutdown.Load()
}

// SetRemoveOwnerOnShutdown sets whether the lifecycler should remove the partition owner on shutdown.
func (l *PartitionInstanceLifecycler) SetRemoveOwnerOnShutdown(remove bool) {
	l.removeOwnerOnShutdown.Store(remove)
}

// GetPartitionState returns the current state of the partition, and the timestamp when the state was
// changed the last time.
// TODO unit test
func (l *PartitionInstanceLifecycler) GetPartitionState(ctx context.Context) (PartitionState, time.Time, error) {
	ring, err := l.getRing(ctx)
	if err != nil {
		return PartitionUnknown, time.Time{}, err
	}

	partition, exists := ring.Partitions[l.cfg.PartitionID]
	if !exists {
		return PartitionUnknown, time.Time{}, ErrPartitionDoesNotExist
	}

	return partition.GetState(), time.Unix(partition.GetStateTimestamp(), 0), nil
}

// ChangePartitionState changes the partition state to toState.
// This function returns ErrPartitionDoesNotExist if the partition doesn't exist,
// and ErrPartitionStateChangeNotAllowed if the state change is not allowed.
// TODO unit test
func (l *PartitionInstanceLifecycler) ChangePartitionState(ctx context.Context, toState PartitionState) error {
	return l.run(func() error {
		err := l.updateRing(ctx, func(ring *PartitionRingDesc) (bool, error) {
			partition, exists := ring.Partitions[l.cfg.PartitionID]
			if !exists {
				return false, ErrPartitionDoesNotExist
			}

			// TODO unit test: fromState == toState
			if partition.State == toState {
				return false, nil
			}

			if !isPartitionStateChangeAllowed(partition.State, toState) {
				return false, errors.Wrapf(ErrPartitionStateChangeNotAllowed, "from %s to %s", partition.State.String(), toState.String())
			}

			return ring.UpdatePartitionState(l.cfg.PartitionID, toState, time.Now()), nil
		})

		if err != nil {
			level.Warn(l.logger).Log("msg", "failed to change partition state", "to_state", toState, "err", err)
		}

		return err
	})
}

func (l *PartitionInstanceLifecycler) starting(ctx context.Context) error {
	if err := l.createPartitionIfNotExist(ctx); err != nil {
		return errors.Wrap(err, "create partition in the ring")
	}

	return nil
}

func (l *PartitionInstanceLifecycler) running(ctx context.Context) error {
	reconcileTicker := time.NewTicker(l.cfg.reconcileInterval)
	defer reconcileTicker.Stop()

	for {
		select {
		case <-reconcileTicker.C:
			// TODO track metrics: reconcile_total and reconcile_failures_total
			l.reconcileOwnedPartition(ctx, time.Now())
			l.reconcileOtherPartitions(ctx, time.Now())

		case f := <-l.actorChan:
			f()

		case <-ctx.Done():
			level.Info(l.logger).Log("msg", "partition ring lifecycler is shutting down", "ring", l.ringName)
			return nil
		}
	}
}

func (l *PartitionInstanceLifecycler) stopping(runningError error) error {
	if runningError != nil {
		return nil
	}

	// Remove the instance from partition owners, if configured to do so.
	if l.RemoveOwnerOnShutdown() {
		err := l.updateRing(context.Background(), func(ring *PartitionRingDesc) (bool, error) {
			return ring.RemoveOwner(l.cfg.InstanceID), nil
		})

		if err != nil {
			level.Error(l.logger).Log("msg", "failed to remove instance from partition owners on shutdown", "instance", l.cfg.InstanceID, "err", err)
		} else {
			level.Info(l.logger).Log("msg", "instance removed from partition owners", "instance", l.cfg.InstanceID)
		}
	}

	// Remove the partition if it's inactive since more than the minimum waiting period.
	if l.cfg.DeleteInactivePartitionAfterDuration > 0 {
		removed := false

		err := l.updateRing(context.Background(), func(ring *PartitionRingDesc) (bool, error) {
			partition, exists := ring.Partitions[l.cfg.PartitionID]
			if !exists {
				return false, nil
			}

			if !partition.IsInactiveSince(time.Now().Add(l.cfg.DeleteInactivePartitionAfterDuration)) {
				return false, nil
			}

			// TODO only if it's empty, otherwise other lifecyclers will start to complain. We have the 2x duration cleanup anyway.
			ring.RemovePartition(l.cfg.PartitionID)
			removed = true
			return true, nil
		})

		if err != nil {
			level.Error(l.logger).Log("msg", "failed to remove inactive partition from the ring", "err", err)
		} else if removed {
			level.Info(l.logger).Log("msg", "inactive partition removed from the ring")
		}
	}

	return nil
}

// run a function within the lifecycler loop.
func (l *PartitionInstanceLifecycler) run(fn func() error) error {
	sc := l.ServiceContext()
	if sc == nil {
		return errors.New("lifecycler not running")
	}

	errCh := make(chan error)
	wrappedFn := func() {
		errCh <- fn()
	}

	select {
	case <-sc.Done():
		return errors.New("lifecycler not running")
	case l.actorChan <- wrappedFn:
		return <-errCh
	}
}

func (l *PartitionInstanceLifecycler) getRing(ctx context.Context) (*PartitionRingDesc, error) {
	in, err := l.store.Get(ctx, l.ringKey)
	if err != nil {
		return nil, err
	}

	return GetOrCreatePartitionRingDesc(in), nil
}

func (l *PartitionInstanceLifecycler) updateRing(ctx context.Context, update func(ring *PartitionRingDesc) (bool, error)) error {
	return l.store.CAS(ctx, l.ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		ringDesc := GetOrCreatePartitionRingDesc(in)

		if changed, err := update(ringDesc); err != nil {
			return nil, false, err
		} else if !changed {
			return nil, false, nil
		}

		return ringDesc, true, nil
	})
}

// TODO rename because it also add the instance as owner
func (l *PartitionInstanceLifecycler) createPartitionIfNotExist(ctx context.Context) error {
	return l.updateRing(ctx, func(ring *PartitionRingDesc) (bool, error) {
		now := time.Now()
		changed := false

		partitionDesc, exists := ring.Partitions[l.cfg.PartitionID]
		if exists {
			level.Info(l.logger).Log("msg", "partition found in the ring", "state", partitionDesc.GetState(), "state_timestamp", partitionDesc.GetState().String(), "tokens", len(partitionDesc.GetTokens()))
		} else {
			level.Info(l.logger).Log("msg", "partition not found in the ring")
		}

		// The partition does not exist:
		// TODO - EDGE case: we shouldn't create the partition if there was a shutdown marker. But then this will make the lifecycler more complicated, because it currently assumes the partition exists.
		if !exists {
			// The partition doesn't exist, so we create a new one. A new partition should always be created
			// in PENDING state.
			ring.AddPartition(l.cfg.PartitionID, PartitionPending, now)
			changed = true
		}

		// Ensure the instance is added as partition owner.
		if ring.AddOrUpdateOwner(l.cfg.InstanceID, OwnerActive, l.cfg.PartitionID, now) {
			changed = true
		}

		return changed, nil
	})
}

// reconcileOwnedPartition reconciles the owned partition.
// This function should be called periodically.
// TODO unit test
func (l *PartitionInstanceLifecycler) reconcileOwnedPartition(ctx context.Context, now time.Time) {
	err := l.updateRing(ctx, func(ring *PartitionRingDesc) (bool, error) {
		partitionID := l.cfg.PartitionID

		partition, exists := ring.Partitions[partitionID]
		if !exists {
			// TODO what should we do here? should be re-created? in which state?
			return false, ErrPartitionDoesNotExist
		}

		// A pending partition should be switched to active if there are enough owners that
		// have been added since more than the waiting period.
		if partition.IsPending() && ring.PartitionOwnersCountUpdatedBefore(partitionID, now.Add(-l.cfg.WaitOwnersDurationOnPending)) >= l.cfg.WaitOwnersCountOnPending {
			level.Info(l.logger).Log("msg", fmt.Sprintf("switching partition state from %s to %s because enough owners have been registered and minimum waiting time has elapsed", PartitionPending.CleanName(), PartitionActive.CleanName()))
			return ring.UpdatePartitionState(partitionID, PartitionActive, now), nil
		}

		return false, nil
	})

	if err != nil {
		level.Warn(l.logger).Log("msg", "failed to reconcile owned partition", "err", err)
	}
}

// reconcileOtherPartitions reconciles other partitions.
// This function should be called periodically.
// TODO unit test
func (l *PartitionInstanceLifecycler) reconcileOtherPartitions(ctx context.Context, now time.Time) {
	err := l.updateRing(ctx, func(ring *PartitionRingDesc) (bool, error) {
		changed := false

		// Delete other inactive partitions after 2x the deletion threshold time has passed.
		// This logic is used to cleanup inactive partitions that may have been left over
		// by non-graceful scale downs.
		if l.cfg.DeleteInactivePartitionAfterDuration > 0 {
			deleteBefore := now.Add(-2 * l.cfg.DeleteInactivePartitionAfterDuration)

			for partitionID, partition := range ring.Partitions {
				if partitionID == l.cfg.PartitionID {
					continue
				}

				if partition.IsInactiveSince(deleteBefore) {
					ring.RemovePartition(partitionID)
				}
			}
		}

		return changed, nil
	})

	if err != nil {
		level.Warn(l.logger).Log("msg", "failed to reconcile other partitions", "err", err)
	}
}

func isPartitionStateChangeAllowed(from, to PartitionState) bool {
	for _, allowed := range allowedPartitionStateChanges[from] {
		if to == allowed {
			return true
		}
	}

	return false
}
