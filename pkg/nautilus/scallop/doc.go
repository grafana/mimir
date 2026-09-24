// SPDX-License-Identifier: AGPL-3.0-only

// Package scallop implements the pure, deterministic Nautilus placement
// planner. It has no clocks, persistence, RPCs, or workload generators:
// callers provide a complete observed Snapshot and receive a projected plan.
package scallop
