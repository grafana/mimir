// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
)

// Compile-time check to ensure the interfaces are satisfied.
var (
	_ ingesterLifecycler           = (*ring.Lifecycler)(nil)
	_ ingesterLifecycler           = (*tokenlessLifecycler)(nil)
	_ ring.BasicLifecyclerDelegate = (*tokenlessLifecycler)(nil)
)

// ingesterLifecycler is the interface satisfied by both *ring.Lifecycler
// and *tokenlessLifecycler for the methods used by the ingester.
type ingesterLifecycler interface {
	services.Service
	http.Handler

	// GetState returns the current state of the instance in the ring.
	GetState() ring.InstanceState

	// GetReadOnlyState returns the read-only state and when it was set.
	GetReadOnlyState() (bool, time.Time)

	// ChangeReadOnlyState changes the read-only state of the instance.
	ChangeReadOnlyState(ctx context.Context, readOnly bool) error

	// CheckReady checks if the instance is ready to serve requests.
	CheckReady(ctx context.Context) error

	// SetUnregisterOnShutdown enables/disables unregistering on shutdown.
	SetUnregisterOnShutdown(enabled bool)

	// ShouldUnregisterOnShutdown returns if unregistering should happen on shutdown.
	ShouldUnregisterOnShutdown() bool

	// SetFlushOnShutdown enables/disables flush on shutdown.
	SetFlushOnShutdown(enabled bool)

	// FlushOnShutdown returns if flush is enabled on shutdown.
	FlushOnShutdown() bool
}

// tokenlessLifecycler wraps ring.BasicLifecycler to provide methods
// matching the classic ring.Lifecycler API, enabling both to satisfy
// the ingesterLifecycler interface. It also implements ring.BasicLifecyclerDelegate
// to handle tokenless ingester registration and flush on shutdown.
type tokenlessLifecycler struct {
	*ring.BasicLifecycler

	logger           log.Logger
	minReadyDuration time.Duration
	finalSleep       time.Duration
	flush            func() // callback to Ingester.Flush()
	flushOnShutdown  *atomic.Bool

	// Ready state tracking (matching classic Lifecycler behavior).
	readyMu    sync.Mutex
	ready      bool
	readySince time.Time
}

// newTokenlessLifecycler creates a new tokenlessLifecycler that encapsulates
// the BasicLifecycler and delegate chain creation. This provides a drop-in replacement
// for the classic ring.Lifecycler in tokenless mode.
func newTokenlessLifecycler(
	cfg ring.BasicLifecyclerConfig,
	ringName, ringKey string,
	ringKV kv.Client,
	minReadyDuration time.Duration,
	finalSleep time.Duration,
	flushOnShutdown bool,
	flush func(),
	logger log.Logger,
	registerer prometheus.Registerer,
) (*tokenlessLifecycler, error) {
	// Create tokenlessLifecycler first (used as innermost delegate).
	lc := &tokenlessLifecycler{
		logger:           logger,
		minReadyDuration: minReadyDuration,
		finalSleep:       finalSleep,
		flush:            flush,
		flushOnShutdown:  atomic.NewBool(flushOnShutdown),
	}

	// Build delegate chain: LeaveOnStoppingDelegate wraps tokenlessLifecycler.
	// During stopping, delegates execute outermost first:
	// 1. LeaveOnStoppingDelegate.OnRingInstanceStopping() - changes state to LEAVING
	// 2. tokenlessLifecycler.OnRingInstanceStopping() - calls Flush() if enabled
	var delegate ring.BasicLifecyclerDelegate
	delegate = lc
	delegate = ring.NewLeaveOnStoppingDelegate(delegate, logger)

	// Create BasicLifecycler with the delegate chain.
	lifecycler, err := ring.NewBasicLifecycler(cfg, ringName, ringKey, ringKV, delegate, logger, registerer)
	if err != nil {
		return nil, err
	}

	// Complete the tokenlessLifecycler by setting the BasicLifecycler reference.
	lc.BasicLifecycler = lifecycler
	return lc, nil
}

// SetUnregisterOnShutdown matches the classic Lifecycler method.
func (w *tokenlessLifecycler) SetUnregisterOnShutdown(enabled bool) {
	w.SetKeepInstanceInTheRingOnShutdown(!enabled)
}

// ShouldUnregisterOnShutdown matches the classic Lifecycler method.
func (w *tokenlessLifecycler) ShouldUnregisterOnShutdown() bool {
	return !w.ShouldKeepInstanceInTheRingOnShutdown()
}

// SetFlushOnShutdown enables/disables flush on shutdown.
// Note: BasicLifecycler doesn't call Flush directly - the caller must handle this.
func (w *tokenlessLifecycler) SetFlushOnShutdown(enabled bool) {
	w.flushOnShutdown.Store(enabled)
}

// FlushOnShutdown returns if flush is enabled on shutdown.
func (w *tokenlessLifecycler) FlushOnShutdown() bool {
	return w.flushOnShutdown.Load()
}

// CheckReady checks if the instance is ready to serve requests.
// This matches the classic Lifecycler behavior with a ready state latch
// and MinReadyDuration delay.
func (w *tokenlessLifecycler) CheckReady(_ context.Context) error {
	w.readyMu.Lock()
	defer w.readyMu.Unlock()

	// Ready state latches: once ready, stay ready.
	if w.ready {
		return nil
	}

	// Check that instance is registered and ACTIVE.
	if err := w.checkReadiness(); err != nil {
		// Reset the min ready duration counter.
		w.readySince = time.Time{}
		return err
	}

	// Honor the min ready duration. The duration counter starts after
	// all readiness checks have passed.
	if w.readySince.IsZero() {
		w.readySince = time.Now()
	}
	if time.Since(w.readySince) < w.minReadyDuration {
		return fmt.Errorf("waiting for %v after being ready", w.minReadyDuration)
	}

	w.ready = true
	return nil
}

// checkReadiness performs the basic readiness checks for tokenless mode.
func (w *tokenlessLifecycler) checkReadiness() error {
	if !w.IsRegistered() {
		return fmt.Errorf("instance not registered in the ring")
	}
	if state := w.GetState(); state != ring.ACTIVE {
		return fmt.Errorf("instance not in ACTIVE state, current state: %s", state)
	}
	return nil
}

// OnRingInstanceRegister is called while the lifecycler is registering the instance.
// For tokenless mode, we return ACTIVE state with no tokens.
func (w *tokenlessLifecycler) OnRingInstanceRegister(_ *ring.BasicLifecycler, _ ring.Desc, instanceExists bool, instanceID string, _ ring.InstanceDesc) (ring.InstanceState, ring.Tokens) {
	level.Info(w.logger).Log("msg", "registering tokenless ingester instance", "instance", instanceID, "exists", instanceExists)
	return ring.ACTIVE, ring.Tokens{}
}

// OnRingInstanceTokens is called once the instance tokens are set and are stable.
// In tokenless mode, this is a no-op since we have no tokens.
func (w *tokenlessLifecycler) OnRingInstanceTokens(_ *ring.BasicLifecycler, _ ring.Tokens) {
	// No tokens expected in tokenless mode - nothing to do.
}

// OnRingInstanceStopping is called while the lifecycler is stopping.
// If flush on shutdown is enabled, we call the flush callback.
// The state transition to LEAVING is handled by LeaveOnStoppingDelegate which wraps this.
func (w *tokenlessLifecycler) OnRingInstanceStopping(_ *ring.BasicLifecycler) {
	if w.flushOnShutdown.Load() && w.flush != nil {
		level.Info(w.logger).Log("msg", "flushing blocks before shutdown")
		w.flush()
	}

	// Sleep to allow metrics to be scraped before shutdown.
	if w.finalSleep > 0 {
		level.Info(w.logger).Log("msg", "lifecycler entering final sleep before shutdown", "final_sleep", w.finalSleep)
		time.Sleep(w.finalSleep)
	}
}

// OnRingInstanceHeartbeat is called while the instance is updating its heartbeat.
// In tokenless mode, this is a no-op.
func (w *tokenlessLifecycler) OnRingInstanceHeartbeat(_ *ring.BasicLifecycler, _ *ring.Desc, _ *ring.InstanceDesc) {
	// Nothing to do in tokenless mode.
}
