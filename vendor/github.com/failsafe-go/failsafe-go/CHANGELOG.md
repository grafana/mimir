## Upcoming Release

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