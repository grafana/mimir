// SPDX-License-Identifier: AGPL-3.0-only

// Package kmeta (short for "kafka metadata") holds Kafka metadata value types shared across packages —
// currently the partition offset types (see PartitionOffsets). It is intentionally a dependency-free leaf
// — it must not import other internal packages — so that other packages with strict import rules can
// import it.
package kmeta
