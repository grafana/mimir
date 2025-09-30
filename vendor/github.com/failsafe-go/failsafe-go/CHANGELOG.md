## Upcoming Release

## 0.8.2

### Improvements

- Improve `UsageTracker` performance.

## 0.8.1

### Improvements

- New `UsageTracker` which can track usage and enforce fairness when prioritizing executions.

## 0.8.0

### Improvements

- New `AdaptiveThrottler` policy.
- Improved gRPC `tap.ServerInHandle` support to perform `CanAcquirePermit` checks on policies, when possible, so as to not block execution.

### API Changes

- Breaking change: the `circuitbreaker.Builder` `WithFailureRateThreshold` method now expects the `failureRateThreshold` parameter to be a `float64`, rather than an int representing a percentage. This brings the circuit breaker's API inline with the rest of the library, where rates are represented with `float64`. The `WithFailureRateThreshold` method will panic if a value < 1 is provided.
- Similarly, the `circuitbreaker.Metrics` `SuccessRate` and `FailureRate` methods now return a `float64` instead of an int representing a percentage.

## 0.7.0

### Improvements

- New `AdaptiveLimiter` policy.
- Added grpc and http integration for adaptive limiters.
- Added support for failsafehttp server Handlers.

### API Changes

- The builder creation methods through the library had their names changed from `Builder()` to `NewBuilder()`, and so on.
- The policy builder types were renamed from, for example: `RetryPolicyBuilder` to `Builder`.

## 0.6.9

### Bug Fixes

- Fixed #73 - Retries should re-read request bodies.

## 0.6.8

### Bug Fixes

- Fixed #65 - Mixing `failsafehttp.RetryPolicy` with `HedgePolicy` causes contexts to be canceled early.

### Improvements

- Added `Context()` to `circuitbreaker.StateChangedEvent`.

## 0.6.7

### Improvements

- Added `HandleErrorTypes` to match errors by type in retry policies, circuit breakers, and fallbacks. This is similar to the matching that `errors.As` does.
- Added `RetryPolicyBuilder.AbortOnErrorTypes`.
- Added `HedgePolicyBuilder.CancelOnErrorTypes`.

### API Changes

- Renamed the `retrypolicy.ExceededError` `LastResult()` to `LastResult` and `LastError()` to `LastError`.

## 0.6.6

### Bug Fixes

- Always expose metrics from old state in `StateChangedEvent`.

## 0.6.5

### Improvements

- Added gRPC unary client, unary server, and server inHandle support.
- Expose `Context()` in event listeners.
- Improve HTTP context cancellation.
- Add `Metrics()` to `circuitbreaker.StateChangedEvent`.
- Default `failsafehttp.RetryPolicyBuilder()` to abort on `context.Canceled`.

## 0.6.4

### Bug Fixes

- Improve bulkhead handling of short max wait times.

## 0.6.3

### Improvements

- Optimized memory usage in time based circuit breakers.

## 0.6.2

### Improvements

- New CachePolicy.

## 0.6.1

### Improvements

- Better support for `HedgePolicy` and `Timeout` composition

## 0.6.0

### Improvements

- Added HTTP support via `failsafehttp.NewRoundTripper`

### Bug Fixes

- Fixed #32 - `RetryPolicy` with no max retries.

## 0.5.0

### Improvements

- Added a new `HedgePolicy`

## 0.4.5

### Bug Fixes

- Fixed #29 - `RetryPolicy` `WithMaxDuration` not working

## 0.4.4

### Improvements

- Added `CircuitBreaker.RemainingDelay()`

### API Changes

- Renamed `retrypolicy.ErrRetriesExceeded` to `retrypolicy.ErrExceeded`
- Renamed `retrypolicy.RetriesExceededError` to `retrypolicy.ExceededError`
- Renamed `circuitbreaker.ErrCircuitBreakerOpen` to `circuitbreaker.ErrOpen`
- Renamed `bulkhead.ErrBulkheadFull` to `bulkhead.ErrFull`
- Renamed `ratelimiter.ErrRateLimitExceeded` to `ratelimiter.ErrExceeded`
- Renamed `timeout.ErrTimeoutExceeded` to `timeout.ErrExceeded`
- Renamed `BulkheadBuilder.OnBulkheadFull` to `OnFull`

## 0.4.2

### Bug Fixes

- Fixed #23 - `RetryPolicy` backoff not computing

## 0.4.1

### Bug Fixes

- Fixed #22 - `RetryPolicy` with `ReturnLastFailure` returning too late

## 0.4.0

### Improvements

- Always cancel Context on when a Timeout policy is exceeded

### API changes

- Rename Fn -> Func and DelayFunction -> DelayFunc
- Change some params from int to uint

## 0.3.1

### Improvements

- Lazily create canceled channels.
- Propagate cancellations to contexts.

## 0.3.0

- Initial Release