## Upcoming Release

## 0.6.9

### Bug Fixes

- Fixed #73 - Retries should re-read request bodies.

## 0.6.8

### Bug Fixes

- Fixed #65 - Mixing failsafehttp.RetryPolicy with HedgePolicy causes contexts to be canceled early.

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
- Expose Context() in event listeners.
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

- Better support for HedgePolicy and Timeout composition

## 0.6.0

### Improvements

- Added HTTP support via `failsafehttp.NewRoundTripper`

### Bug Fixes

- Fixed #32 - RetryPolicy with no max retries.

## 0.5.0

### Improvements

- Added a new `HedgePolicy`

## 0.4.5

### Bug Fixes

- Fixed #29 - RetryPolicy `WithMaxDuration` not working

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

- Fixed #23 - RetryPolicy backoff not computing

## 0.4.1

### Bug Fixes

- Fixed #22 - RetryPolicy with ReturnLastFailure returning too late

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