// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"errors"
	"flag"
	"time"
)

// Config holds all configuration for the segmentation label analyzer.
type Config struct {
	TenantID string

	MimirAddress  string
	MimirUsername string
	MimirPassword string

	LokiAddress  string
	LokiUsername string
	LokiPassword string

	Namespace string

	// UserQueriesDuration is the time range for analyzing user queries (from query-frontend).
	// User queries are more varied, so we need a longer time range.
	UserQueriesDuration time.Duration

	// RuleQueriesDuration is the time range for analyzing rule queries (from ruler-query-frontend).
	// Rule queries are repetitive, so a shorter time range is sufficient.
	RuleQueriesDuration time.Duration
}

// RegisterFlags registers the configuration flags.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.TenantID, "tenant-id", "", "Tenant ID to analyze.")
	f.StringVar(&cfg.MimirAddress, "mimir-address", "", "Mimir endpoint URL (e.g., https://mimir.example.com).")
	f.StringVar(&cfg.MimirUsername, "mimir-username", "", "Username for Mimir basic auth.")
	f.StringVar(&cfg.MimirPassword, "mimir-password", "", "Password for Mimir basic auth.")
	f.StringVar(&cfg.LokiAddress, "loki-address", "", "Loki endpoint URL (e.g., https://loki.example.com).")
	f.StringVar(&cfg.LokiUsername, "loki-username", "", "Username for Loki basic auth.")
	f.StringVar(&cfg.LokiPassword, "loki-password", "", "Password for Loki basic auth.")
	f.StringVar(&cfg.Namespace, "namespace", "", "Kubernetes namespace for log filtering.")
	f.DurationVar(&cfg.UserQueriesDuration, "user-queries-duration", time.Hour, "Time range for analyzing user queries (from query-frontend).")
	f.DurationVar(&cfg.RuleQueriesDuration, "rule-queries-duration", 5*time.Minute, "Time range for analyzing rule queries (from ruler-query-frontend).")
}

// Validate checks that the configuration is valid.
func (cfg *Config) Validate() error {
	if cfg.TenantID == "" {
		return errors.New("-tenant-id is required")
	}
	if cfg.MimirAddress == "" {
		return errors.New("-mimir-address is required")
	}
	if cfg.LokiAddress == "" {
		return errors.New("-loki-address is required")
	}
	if cfg.Namespace == "" {
		return errors.New("-namespace is required")
	}
	return nil
}
