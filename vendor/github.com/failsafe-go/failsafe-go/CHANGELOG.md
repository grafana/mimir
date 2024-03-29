## Upcoming Release

## 0.6.1

## Improvements

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