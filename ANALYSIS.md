# Sample-Too-Old Rejection Analysis

## Problem Statement

Ingesters are rejecting samples with `sample-too-old` errors even though the distributor should be the sole enforcement point for timestamp validation. This causes unnecessary cross-tenant data leakage risks and inconsistent behavior.

## Current Behavior

### Distributor Validation (pkg/distributor/validate.go:371-376)

```go
if cfg.pastGracePeriod > 0 && model.Time(s.TimestampMs) < now.Add(-cfg.pastGracePeriod).Add(-cfg.outOfOrderTimeWindow) {
    m.tooFarInPast.WithLabelValues(userID, group).Inc()
    cat.IncrementDiscardedSamples(ls, 1, reasonTooFarInPast, now.Time())
    unsafeMetricName, _ := extract.UnsafeMetricNameFromLabelAdapters(ls)
    return fmt.Errorf(sampleTimestampTooOldMsgFormat, s.TimestampMs, unsafeMetricName)
}
```

**Check:** `timestamp < now - pastGracePeriod - outOfOrderTimeWindow`

### Ingester/TSDB Validation (vendor/.../tsdb/head_append.go:681-692)

```go
// The sample cannot go in the in-order chunk. Check if it can go in the out-of-order chunk.
if oooTimeWindow > 0 && t >= headMaxt-oooTimeWindow {
    return true, headMaxt - t, nil
}

// The sample cannot go in both in-order and out-of-order chunk.
if oooTimeWindow > 0 {
    return true, headMaxt - t, storage.ErrTooOldSample
}
if t < minValidTime {
    return false, headMaxt - t, storage.ErrOutOfBounds
}
return false, headMaxt - t, storage.ErrOutOfOrderSample
```

**Check:** `timestamp < headMaxt - oooTimeWindow` (when OOO enabled)

## Root Cause

The distributor validates against **wall clock time** (`now`), but the ingester validates against **per-series maximum timestamp** (`headMaxt`). These can diverge significantly:

1. **Scenario 1**: A series stops receiving data for a while
   - `headMaxt` for that series is frozen at the last sample timestamp
   - New samples arriving later will have `timestamp > now - pastGracePeriod - oooTimeWindow` (pass distributor)
   - But `timestamp < headMaxt - oooTimeWindow` (fail ingester with `ErrTooOldSample`)

2. **Scenario 2**: Samples arrive out of order
   - A late-arriving sample with old timestamp passes distributor check
   - But if the series has already received newer samples, `headMaxt` is higher
   - Sample fails ingester check

## Error Types

From `vendor/.../prometheus/storage/interface.go:30-38`:

- **`ErrOutOfBounds`**: When OOO support is **disabled** and sample is older than `minValidTime`
- **`ErrTooOldSample`**: When OOO support is **enabled** but sample is outside the time window (`t < headMaxt - oooTimeWindow`)
- **`ErrOutOfOrderSample`**: When sample timestamp is less than the last sample timestamp

## Impact

1. **Cross-tenant data leakage risk**: Ingester uses unsafe memory tricks with pooled buffers. Samples that bypass distributor validation but fail at ingester may retain references to shared buffers.

2. **Inconsistent behavior**: Users see `sample-too-old` errors even though their samples should be valid according to configured limits.

3. **Metrics confusion**: `sample-too-old` metrics increment at ingester level, making it harder to understand where rejection is happening.

## Proposed Solution

The distributor cannot perfectly predict `headMaxt` for each series without maintaining series state. However, we can improve the situation:

### Option 1: Conservative Distributor Check (Recommended)

Make the distributor more conservative by checking against a tighter bound that accounts for the fact that `headMaxt` might be ahead of `now - pastGracePeriod`:

```go
// Account for the fact that headMaxt might be ahead of now - pastGracePeriod
// by an additional margin (e.g., creationGracePeriod)
effectiveMinTime := now.Add(-cfg.pastGracePeriod).Add(-cfg.outOfOrderTimeWindow).Add(-cfg.creationGracePeriod)
if cfg.pastGracePeriod > 0 && model.Time(s.TimestampMs) < effectiveMinTime {
    // reject
}
```

This ensures samples that pass distributor validation are more likely to be accepted by ingesters.

### Option 2: Remove Ingester Validation

Since the distributor is meant to be the enforcement point, remove or relax the ingester's timestamp validation. However, this requires careful consideration of TSDB invariants.

### Option 3: Distributor Consults Usage-Tracker

The usage-tracker could maintain per-series `headMaxt` information and the distributor could query it. This is more complex but provides accurate validation.

## Recommendation

Implement **Option 1** as a short-term fix to reduce `sample-too-old` rejections at the ingester level. The conservative check ensures better alignment between distributor and ingester validation without requiring architectural changes.

For a long-term solution, consider whether the ingester should reject samples at all when the distributor is the designated enforcement point, or if the usage-tracker should provide per-series state to the distributor.

## Files to Modify

1. `pkg/distributor/validate.go` - Update `validateSample()` and `validateSampleHistogram()` to use more conservative timestamp checks
2. `pkg/distributor/validate_test.go` - Add tests for edge cases
3. `CHANGELOG.md` - Document the bugfix

## Testing Strategy

1. Unit tests for edge cases where `headMaxt` diverges from wall clock
2. Integration tests with delayed samples
3. Verify metrics show reduced `sample-too-old` rejections at ingester level
