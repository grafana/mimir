// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/tsdb"

	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util/atomicfs"
)

const (
	idleTSDBStateFilename = "idle-state.json"
	idleTSDBStateVersion  = 1
	idleTSDBActive        = "active"
	idleTSDBEmpty         = "empty"
)

type idleTSDBState struct {
	Version      int         `json:"version"`
	State        string      `json:"state"`
	EmptySinceMs int64       `json:"empty_since_ms,omitempty"`
	BlockIDs     []ulid.ULID `json:"block_ids,omitempty"`
}

type idleTSDBStateMetrics struct {
	restored prometheus.Counter
	failures *prometheus.CounterVec
}

func newIdleTSDBStateMetrics(reg prometheus.Registerer) *idleTSDBStateMetrics {
	m := &idleTSDBStateMetrics{
		restored: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_idle_tsdb_state_restorations_total",
			Help: "Total number of empty TSDB idle timestamps restored from local disk.",
		}),
		failures: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingester_idle_tsdb_state_failures_total",
			Help: "Total number of failures reading or persisting local TSDB idle state.",
		}, []string{"operation"}),
	}
	m.failures.WithLabelValues("read")
	m.failures.WithLabelValues("write")
	return m
}

func persistIdleTSDBStateEnabled(cfg mimir_tsdb.TSDBConfig) bool {
	return cfg.PersistIdleStateEnabled && cfg.CloseIdleTSDBTimeout > 0 &&
		(cfg.IsBlocksShippingEnabled() || cfg.CloseIdleTSDBWhenShippingDisabled)
}

// idleTSDBStateTracker retains empty-head age without changing compaction's lastUpdate.
// Its gate excludes appends while establishing empty state; mu serializes file updates.
type idleTSDBStateTracker struct {
	gate sync.RWMutex
	mu   sync.Mutex

	dir       string
	logger    log.Logger
	metrics   *idleTSDBStateMetrics
	now       func() time.Time
	persist   func(string, idleTSDBState) error
	record    idleTSDBState
	persisted bool
}

func newIdleTSDBStateTracker(dir string, logger log.Logger, metrics *idleTSDBStateMetrics) *idleTSDBStateTracker {
	return &idleTSDBStateTracker{
		dir:     dir,
		logger:  logger,
		metrics: metrics,
		now:     time.Now,
		persist: writeIdleTSDBState,
	}
}

// recover must run after WAL replay and before startup compaction can empty the head.
func (s *idleTSDBStateTracker) recover(db *tsdb.DB) {
	record, err := readIdleTSDBState(s.dir, s.now())
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		s.metrics.failures.WithLabelValues("read").Inc()
		level.Warn(s.logger).Log("msg", "unable to restore TSDB idle state", "err", err)
		return
	}

	if record.State == idleTSDBActive {
		s.record, s.persisted = record, true
		return
	}

	// Older binaries do not invalidate idle-state.json before writing. Such writes
	// either survive in the replayed head or produce new immutable block identities.
	// Blocks removed by retention need not invalidate the saved empty age.
	if db.Head().NumSeries() != 0 || !containsIdleTSDBBlocks(record.BlockIDs, idleTSDBBlockIDs(db)) {
		return
	}

	s.record, s.persisted = record, true
	s.metrics.restored.Inc()
}

// activate must succeed before committing accepted samples or exemplars.
// The caller holds gate's read side, so observation cannot race with this append.
func (s *idleTSDBStateTracker) activate() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.persisted && s.record.State == idleTSDBActive {
		return nil
	}

	record := idleTSDBState{Version: idleTSDBStateVersion, State: idleTSDBActive}
	if err := s.persist(s.dir, record); err != nil {
		s.metrics.failures.WithLabelValues("write").Inc()
		return fmt.Errorf("persist active TSDB idle state: %w", err)
	}
	s.record, s.persisted = record, true
	return nil
}

// observeEmpty is called with appends excluded and an empty head verified.
func (s *idleTSDBStateTracker) observeEmpty(blockIDs []ulid.ULID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.persisted && s.record.State == idleTSDBEmpty && containsIdleTSDBBlocks(s.record.BlockIDs, blockIDs) {
		return nil
	}

	// Within this process activate excludes intervening accepted writes. Thus a
	// compaction of existing local blocks may refresh their identities without
	// restarting the empty period. The same applies to startup compaction after a
	// successful recovery. Retain this time across persistence retries as well.
	emptySince := s.record.EmptySinceMs
	if s.record.State != idleTSDBEmpty || emptySince <= 0 {
		emptySince = s.now().UnixMilli()
	}
	s.record = idleTSDBState{
		Version:      idleTSDBStateVersion,
		State:        idleTSDBEmpty,
		EmptySinceMs: emptySince,
		BlockIDs:     blockIDs,
	}
	s.persisted = false
	if err := s.persist(s.dir, s.record); err != nil {
		s.metrics.failures.WithLabelValues("write").Inc()
		return fmt.Errorf("persist empty TSDB idle state: %w", err)
	}
	s.persisted = true
	return nil
}

func (s *idleTSDBStateTracker) isIdle(timeout time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.persisted && s.record.State == idleTSDBEmpty &&
		time.UnixMilli(s.record.EmptySinceMs).Add(timeout).Before(s.now())
}

func (u *userTSDB) observeEmptyTSDB() error {
	if u.idleState == nil || u.Head().NumSeries() != 0 {
		return nil
	}

	// Lock order is gate -> stateMtx -> metadata mu. Append completion never
	// takes stateMtx while holding metadata mu, and the gate waits without holding
	// either of those locks. Queries do not take this gate.
	u.idleState.gate.Lock()
	defer u.idleState.gate.Unlock()
	u.stateMtx.RLock()
	defer u.stateMtx.RUnlock()
	if u.state != active || u.Head().NumSeries() != 0 {
		return nil
	}
	return u.idleState.observeEmpty(idleTSDBBlockIDs(u.db))
}

func idleTSDBBlockIDs(db *tsdb.DB) []ulid.ULID {
	blocks := db.Blocks()
	ids := make([]ulid.ULID, 0, len(blocks))
	for _, b := range blocks {
		ids = append(ids, b.Meta().ULID)
	}
	slices.SortFunc(ids, func(a, b ulid.ULID) int { return a.Compare(b) })
	return ids
}

func containsIdleTSDBBlocks(recorded, current []ulid.ULID) bool {
	known := make(map[ulid.ULID]struct{}, len(recorded))
	for _, id := range recorded {
		known[id] = struct{}{}
	}
	for _, id := range current {
		if _, ok := known[id]; !ok {
			return false
		}
	}
	return true
}

func readIdleTSDBState(dir string, now time.Time) (idleTSDBState, error) {
	data, err := os.ReadFile(filepath.Join(dir, idleTSDBStateFilename))
	if err != nil {
		return idleTSDBState{}, err
	}
	var record idleTSDBState
	if err := json.Unmarshal(data, &record); err != nil {
		return idleTSDBState{}, fmt.Errorf("decode TSDB idle state: %w", err)
	}
	if record.Version != idleTSDBStateVersion {
		return idleTSDBState{}, fmt.Errorf("unsupported TSDB idle state version %d", record.Version)
	}
	switch record.State {
	case idleTSDBActive:
	case idleTSDBEmpty:
		if record.EmptySinceMs <= 0 || record.EmptySinceMs > now.UnixMilli() {
			return idleTSDBState{}, fmt.Errorf("invalid empty TSDB timestamp %d", record.EmptySinceMs)
		}
	default:
		return idleTSDBState{}, fmt.Errorf("invalid TSDB idle state %q", record.State)
	}
	return record, nil
}

func writeIdleTSDBState(dir string, record idleTSDBState) error {
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode TSDB idle state: %w", err)
	}
	return atomicfs.CreateFile(filepath.Join(dir, idleTSDBStateFilename), bytes.NewReader(data))
}
