---
aliases:
  - ../configuring/configuring-ingester-adaptive-limiters/
description: Learn about ingester adaptive limiters.
menuTitle: Ingester adaptive limiters
title: About Grafana Mimir ingester adaptive limiters
---

# About Grafana Mimir ingester adaptive limiters

Mimir's adaptive concurrency limiters can be used to guard against ingester overload, by automatically adapting concurreny limits based on indications of overload.

## How do adaptive limiters work?

Adaptive limiters detect overload by observing response times, inflight requests, and throughput.

When recent response times increase significantly relative to the longer term trend, an adaptive limiter will temporarily decrease concurrency limits to avoid potential overload. Similarly, when increasing inflight requests correlate with flat or decreasing throughput and increasing response times, this is taken as a sign of overload and the concurrency limit is decreased.

## How do adaptive limiters behave in practice?

When an ingester is not overloaded, the concurrency limit will increase to some multiple of current inflight requests, based on the `max-limit-factor`. This provides headroom for bursts of requests without being too high to lower quickly if overload is detected.

When ingester overload is detected, the limit will gradually decrease to the concurrent request processing capacity of whichever resource is overloaded, then will oscillate around that level so long as the overload continues.

## Request queueing

Since inflight limits are, at most, a multiple of the current inflight ingester requests, some queueing of requests will occur when an ingester's adaptive limiter is full. This allows short bursts of requests to be tolerated while still keeping the number of inflight requests under control. The amount of blocking that is allowed is based on the `initial-rejection-factor` and `max-rejection-factor`, and requests will only queue when the limiter is full.

## Request rejection

Ingesters support separate adaptive limiters for push and read requests since these may be overloaded by different resources at different times. When ingesters are heavily loaded these limiters may be full, and when the load is sustained, queues may fill up as well. When this happens, requests may be rejected, with a preference for rejecting read requests being before push requests.

## Configure Grafana Mimir ingester adaptive limiters

To enable Grafana Mimir ingester push and read adaptive limiters, set `-ingester.push-adaptive-limiter.enabled=true` and `-ingester.read-adaptive-limiter.enabled=true`. You can enable the adaptive limiters independently. Enabling one of them does not require enabling the other one.

### Primary configuration

When enabled, adaptive limiters come with some default configurations which should work well for many workloads. First among these are configurations for the short and long response time windows. These track how changes in recent, short-term response times compare to long-term response times.

When requests are processed by ingesters, their response times are first aggregated in the short window. A quantile from these is regularly taken once the window is full and added to the long window. By default, the minimum duration of the short window is `1s`, the minimum number of responses that must be observed is `50`, and the aggregated response time quantile that's taken is `.9`. The long window defaults to `60` measurements, which are smoothed over time. These values can be changed via `short-window-min-duration`, `short-window-max-duration`, `short-window-min-samples`, `sample-quantile`, and `long-window`.

The inflight request limit has a default min and max range from `2` to `200`, and an initial value of `20`. These can be changed via `min-inflight-limit`, `max-inflight-limit`, and `initial-inflight-limit`. Additionally, the inflight limit will ever only increase to a multiple of the current inflight requests, which defaults to `5.0`. This can be changed via `max-limit-factor`.

### Additional configuration

While response times are the primary mechanism for detecting overload, a secondary mechanism is tracking the correlation between inflight requests and throughput. The default size of this correlation window is `50` samples, which can be changed via `correlation-window`.

The amount of queueing that is allowed when a limiter is full is based on the inflight request limit. By default, requests will begin to be rejected instead of queued when the queue reaches `2` times the inflight limit, and all requests will be rejected when the queue reaches `3` times the inflight limit. These can be configured via `initial-rejection-factor` and `max-rejection-factor`.

Rejection rates, which are based on recent limiter throughput statistics, are computed at `1s` intervals by default for all limiters. This can be adjusted via `-ingester.rejection-prioritizer.calibration-interval`.

## Grafana Mimir ingester adaptive limiter metrics

Grafana Mimir ingester adaptive limiters add several metrics, which aren't part of any API guarantee and can be changed at any time:

- `cortex_ingester_adaptive_limiter_inflight_requests`: Gauge showing the current number of requests that are inflight within the adaptive limiter. It contains the `request_type` label which is either `push` or `read`.
- `cortex_ingester_adaptive_limiter_inflight_limit`: Gauge showing the current inflight request limit. It contains the `request_type` label which is either `push` or `read`.
- `cortex_ingester_adaptive_limiter_blocked_requests`: Gauge showing the current number of requests that are queued waiting on the limiter. It contains the `request_type` label which is either `push` or `read`.
- `cortex_ingester_rejection_rate`: Gauge showing the current rate that new requests will be rejected at.
