// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/grafana/dskit/flagext"
)

// Authentication type constants.
const (
	AuthTypeBasicAuth = "basic-auth"
	AuthTypeTrust     = "trust"
)

// Config holds all configuration for the segmentation label analyzer.
type Config struct {
	TenantID string

	MimirAddress  string
	MimirAuthType string // "basic-auth" or "trust"
	MimirUsername string
	MimirPassword string

	LokiAddress  string
	LokiAuthType string // "basic-auth" or "trust"
	LokiUsername string
	LokiPassword string

	Namespace string

	// UserQueriesStart is the start time for analyzing user queries.
	UserQueriesStart flagext.Time
	// UserQueriesEnd is the end time for analyzing user queries.
	UserQueriesEnd flagext.Time

	// RuleQueriesStart is the start time for analyzing rule queries.
	RuleQueriesStart flagext.Time
	// RuleQueriesEnd is the end time for analyzing rule queries.
	RuleQueriesEnd flagext.Time

	// CacheEnabled enables file-based caching to speed up repeated runs during development.
	CacheEnabled bool

	// CacheDir is the directory to store cache files.
	CacheDir string
}

// RegisterFlags registers the configuration flags.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.TenantID, "tenant-id", "", "Tenant ID to analyze.")
	f.StringVar(&cfg.MimirAddress, "mimir-address", "", "Mimir endpoint URL (e.g., https://mimir.example.com).")
	f.StringVar(&cfg.MimirAuthType, "mimir-auth-type", AuthTypeBasicAuth, "Mimir authentication type: 'basic-auth' or 'trust'. When 'trust', tenant ID is passed via X-Scope-OrgID header.")
	f.StringVar(&cfg.MimirUsername, "mimir-username", "", "Username for Mimir basic auth.")
	f.StringVar(&cfg.MimirPassword, "mimir-password", "", "Password for Mimir basic auth.")
	f.StringVar(&cfg.LokiAddress, "loki-address", "", "Loki endpoint URL (e.g., https://loki.example.com).")
	f.StringVar(&cfg.LokiAuthType, "loki-auth-type", AuthTypeBasicAuth, "Loki authentication type: 'basic-auth' or 'trust'. When 'trust', tenant ID is passed via X-Scope-OrgID header.")
	f.StringVar(&cfg.LokiUsername, "loki-username", "", "Username for Loki basic auth.")
	f.StringVar(&cfg.LokiPassword, "loki-password", "", "Password for Loki basic auth.")
	f.StringVar(&cfg.Namespace, "namespace", "", "Kubernetes namespace for log filtering.")

	// Set UTC hour-aligned defaults for time flags.
	currentHour := time.Now().UTC().Truncate(time.Hour)
	cfg.UserQueriesEnd = flagext.Time(currentHour)
	cfg.UserQueriesStart = flagext.Time(currentHour.Add(-time.Hour))
	cfg.RuleQueriesEnd = flagext.Time(currentHour)
	cfg.RuleQueriesStart = flagext.Time(currentHour.Add(-5 * time.Minute))

	f.Var(&cfg.UserQueriesStart, "user-queries-start", "Start time for user queries (default: 1h before current UTC hour).")
	f.Var(&cfg.UserQueriesEnd, "user-queries-end", "End time for user queries (default: current UTC hour).")
	f.Var(&cfg.RuleQueriesStart, "rule-queries-start", "Start time for rule queries (default: 5m before current UTC hour).")
	f.Var(&cfg.RuleQueriesEnd, "rule-queries-end", "End time for rule queries (default: current UTC hour).")

	f.BoolVar(&cfg.CacheEnabled, "cache-enabled", false, "Enable file-based caching to speed up repeated runs (for development).")
	f.StringVar(&cfg.CacheDir, "cache-dir", "/tmp/segmentation-label-analyzer-cache", "Directory to store cache files.")
}

// Validate checks that the configuration is valid.
func (cfg *Config) Validate() error {
	if cfg.TenantID == "" {
		return fmt.Errorf("-tenant-id is required")
	}
	if cfg.MimirAddress == "" {
		return fmt.Errorf("-mimir-address is required")
	}
	if cfg.MimirAuthType != AuthTypeBasicAuth && cfg.MimirAuthType != AuthTypeTrust {
		return fmt.Errorf("-mimir-auth-type must be '%s' or '%s'", AuthTypeBasicAuth, AuthTypeTrust)
	}
	if cfg.LokiAddress == "" {
		return fmt.Errorf("-loki-address is required")
	}
	if cfg.LokiAuthType != AuthTypeBasicAuth && cfg.LokiAuthType != AuthTypeTrust {
		return fmt.Errorf("-loki-auth-type must be '%s' or '%s'", AuthTypeBasicAuth, AuthTypeTrust)
	}
	if cfg.Namespace == "" {
		return fmt.Errorf("-namespace is required")
	}
	return nil
}
