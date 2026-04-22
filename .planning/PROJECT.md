# Mimirtool Backfill Pre-Verification

## What This Is

A client-side pre-verification step for `mimirtool backfill` that validates TSDB blocks before pushing them to a remote Mimir cluster. Today the backfill client at `pkg/mimirtool/client/backfill.go` uploads blocks without any local validation beyond parsing `meta.json`. This project adds a pluggable verification module that fails fast on malformed or undesirable blocks before any bytes hit the network.

## Core Value

Mimirtool users don't upload blocks that will be rejected by the server or that create avoidable load on downstream compactors.

## Requirements

### Validated

(None yet — ship to validate.)

### Active

- [ ] Pluggable verification framework in `pkg/mimirtool/backfill/` that runs before the upload loop in `MimirClient.Backfill()`.
- [ ] Well-formed block check — `meta.json` valid, index readable, chunk segments present and consistent.
- [ ] 24-hour block-duration check with UTC-day alignment (`MinTime % 24h == 0` and `MaxTime - MinTime == 24h`).
- [ ] Clear error reporting so users know which block failed which check and why.
- [ ] Extension seams for future batch-level checks (duplicate-day detection, overlap detection, etc.).

### Out of Scope

- Server-side verification changes — Mimir's server-side block-upload validation (`pkg/mimirtool/client/backfill.go:129`) already exists and is not being modified here.
- Block repair / transformation — we only verify, never mutate or rewrite blocks.
- Duplicate-day detection and overlap detection — mentioned as future expansion, but not in v1. Framework must leave room; checks themselves land later.
- Changes to `pkg/mimirtool/backfill/backfill.go` (the block *creation* path) — only the upload path (`client/backfill.go`) is in scope.

## Context

- Mimirtool is Mimir's operator CLI (`pkg/mimirtool/`, `cmd/mimirtool/`), adapted from cortex-tools.
- Backfill uploads go through a 4-call API dance: `start → files → finish → check` (see `client/backfill.go`).
- Mimir's compactor consolidates small blocks into larger ones. Feeding it 2-hour blocks (the Prometheus default) instead of 24-hour blocks multiplies the compaction work. That's the "harm to the remote system" we want to prevent.
- Prometheus `tsdb.OpenBlock` and Thanos `block.VerifyIndex` already exist upstream and can be reused rather than reimplemented.
- Existing code relevant to verification: `pkg/storage/tsdb/block/` (Mimir's block package, used by the server-side uploader), `pkg/mimirtool/client/backfill.go:182-242` (`GetBlockMeta`, the current cursory validation).

## Constraints

- **Tech stack**: Go, same module layout as the rest of Mimir. Must satisfy `make lint`, `make test`, `goimports -local github.com/grafana/mimir`.
- **Dependencies**: Prefer existing Prometheus / Thanos / internal Mimir block primitives over new code. Do not vendor new deps unless unavoidable.
- **Compatibility**: `mimirtool backfill` is a public CLI. New checks must default to strict (fail-fast) but keep an escape hatch is a gray area for the spec phase.
- **Changelog**: v1 delivery needs a `[FEATURE]` entry with a PR number from `./tools/github-next-pr-number.sh`.
- **Provenance**: The backfill code has cortex-tools provenance headers. Respect them in new files.

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Pre-verify client-side, not replace server-side | Server validation is authoritative; client-side is fail-fast UX. | — Pending |
| Single phase, not multi-milestone | Scope is one coherent feature; splitting adds overhead. | — Pending |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `/gsd-transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `/gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-04-22 after initialization*
