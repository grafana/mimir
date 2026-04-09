// SPDX-License-Identifier: AGPL-3.0-only

package remotewrite

import (
	"fmt"
	"hash/fnv"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/multierror"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/storage"
)

// Manager manages per-tenant Storage instances (one per Config).
// It reconciles the set of active Storages whenever AppendablesForTenant is called,
// creating new ones and stopping removed ones as the tenant's config changes.
type Manager struct {
	walBaseDir    string
	flushDeadline time.Duration
	reg           prometheus.Registerer
	logger        log.Logger

	mu       sync.Mutex
	storages map[string]map[string]*Storage // tenantID → configName → *Storage

	activeConfigs *prometheus.GaugeVec
}

// NewManager creates a Manager.
// walBaseDir is the root directory for all per-tenant WALs
// (each Storage gets <walBaseDir>/<tenantID>/<configName>/).
func NewManager(walBaseDir string, flushDeadline time.Duration, logger log.Logger, reg prometheus.Registerer) *Manager {
	activeConfigs := promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_ruler_remote_write_active_configs",
		Help: "Number of active remote write configs per tenant.",
	}, []string{"user"})

	return &Manager{
		walBaseDir:    walBaseDir,
		flushDeadline: flushDeadline,
		reg:           reg,
		logger:        logger,
		storages:      make(map[string]map[string]*Storage),
		activeConfigs: activeConfigs,
	}
}

// AppendablesForTenant reconciles the set of Storages for userID against cfgs:
//   - Storages whose config name is no longer in cfgs are stopped and removed.
//   - Configs that don't have a Storage yet get one created.
//
// It returns one storage.Appendable per active Config.
func (m *Manager) AppendablesForTenant(userID string, cfgs []Config) ([]storage.Appendable, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(cfgs) == 0 {
		m.stopTenantStorages(userID)
		return nil, nil
	}

	existing := m.storages[userID]
	if existing == nil {
		existing = make(map[string]*Storage)
		m.storages[userID] = existing
	}

	// Build a set of desired config names.
	desired := make(map[string]struct{}, len(cfgs))
	for _, cfg := range cfgs {
		desired[cfg.Name] = struct{}{}
	}

	// Stop storages whose config was removed.
	for name, s := range existing {
		if _, ok := desired[name]; !ok {
			level.Info(m.logger).Log("msg", "stopping removed remote write storage", "user", userID, "remote", name)
			s.Stop()
			delete(existing, name)
		}
	}

	// Create storages for new configs.
	var errs multierror.MultiError
	for _, cfg := range cfgs {
		if _, ok := existing[cfg.Name]; ok {
			continue
		}
		if err := cfg.Validate(); err != nil {
			errs.Add(fmt.Errorf("invalid remote write config %q for tenant %q: %w", cfg.Name, userID, err))
			continue
		}
		walDir := filepath.Join(m.walBaseDir, userID, cfg.Name)
		// Wrap with both user and remote labels so WAL / QueueManager metrics
		// (registered internally by Prometheus) don't collide across storages.
		tenantReg := prometheus.WrapRegistererWith(
			prometheus.Labels{"user": userID, "remote": cfg.Name},
			m.reg,
		)
		s, err := newStorage(cfg, walDir, m.flushDeadline, tenantReg, m.logger)
		if err != nil {
			errs.Add(fmt.Errorf("creating remote write storage %q for tenant %q: %w", cfg.Name, userID, err))
			continue
		}
		level.Info(m.logger).Log("msg", "started remote write storage", "user", userID, "remote", cfg.Name, "url", cfg.URL.String())
		existing[cfg.Name] = s
	}

	m.activeConfigs.WithLabelValues(userID).Set(float64(len(existing)))

	appendables := make([]storage.Appendable, 0, len(existing))
	for _, s := range existing {
		appendables = append(appendables, s)
	}
	// Return the successfully-started appendables even when some configs failed.
	// The caller can log the error and still use the partial set.
	return appendables, errs.Err()
}

// AdHocAppendable returns (or lazily creates) a Storage for a custom URL
// specified in a rule group's remote_write_urls field.
// walDir defaults to <walBaseDir>/<userID>/<urlHash>.
func (m *Manager) AdHocAppendable(userID, url string) (storage.Appendable, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	existing := m.storages[userID]
	if existing == nil {
		existing = make(map[string]*Storage)
		m.storages[userID] = existing
	}

	// Use URL as name (hashed to keep dir names filesystem-safe).
	name := adHocName(url)
	if s, ok := existing[name]; ok {
		return s, nil
	}

	cfg := Config{Name: name}
	if err := cfg.URL.Set(url); err != nil {
		return nil, fmt.Errorf("parsing custom remote write URL %q: %w", url, err)
	}

	walDir := filepath.Join(m.walBaseDir, userID, name)
	tenantReg := prometheus.WrapRegistererWith(prometheus.Labels{"user": userID, "remote": name}, m.reg)
	s, err := newStorage(cfg, walDir, m.flushDeadline, tenantReg, m.logger)
	if err != nil {
		return nil, fmt.Errorf("creating ad-hoc remote write storage for URL %q, tenant %q: %w", url, userID, err)
	}

	level.Info(m.logger).Log("msg", "started ad-hoc remote write storage", "user", userID, "url", url)
	existing[name] = s
	return s, nil
}

// Stop stops all Storages across all tenants.
func (m *Manager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for userID, tenantStorages := range m.storages {
		for name, s := range tenantStorages {
			level.Info(m.logger).Log("msg", "stopping remote write storage", "user", userID, "remote", name)
			s.Stop()
		}
	}
	m.storages = make(map[string]map[string]*Storage)
}

// stopTenantStorages stops and removes all Storages for a tenant.
// Must be called with m.mu held.
func (m *Manager) stopTenantStorages(userID string) {
	for name, s := range m.storages[userID] {
		level.Info(m.logger).Log("msg", "stopping remote write storage (tenant config removed)", "user", userID, "remote", name)
		s.Stop()
		delete(m.storages[userID], name)
	}
	delete(m.storages, userID)
	m.activeConfigs.DeleteLabelValues(userID)
}

// adHocName returns a filesystem-safe name derived from a URL.
func adHocName(url string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(url))
	return fmt.Sprintf("adhoc-%016x", h.Sum64())
}
