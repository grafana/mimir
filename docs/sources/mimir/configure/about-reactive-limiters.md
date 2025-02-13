---
aliases:
  - ../configuring/configuring-reactive-limiters/
description: Learn about reactive limiters.
menuTitle: Reactive limiters
title: About Grafana Mimir reactive limiters
---

# About Grafana Mimir reactive limiters

Mimir's reactive concurrency limiters automatically adapt concurrency limits based on indications of overload. You can use reactive limiters to guard against ingester overload.

## How do reactive limiters work?

Reactive limiters detect overload by observing response times, inflight requests, and throughput.

When recent response times increase significantly relative to the longer term trend, an reactive limiter temporarily decreases concurrency limits to avoid potential overload. Similarly, when increasing inflight requests correlate with flat or decreasing throughput and increasing response times, this indicates overload and causes an reactive limiter to decrease concurrency limits.

When an ingester is not overloaded, the concurrency limit increases to a multiple of current inflight requests, based on the `max-limit-factor`. This provides headroom for bursts of requests without being too high to lower quickly if overload is detected.

When ingester overload is detected, the limit gradually decreases to the concurrent request processing capacity of whichever resource is overloaded, then oscillates around that level, as long as the overload continues.

### Request queueing

Since inflight limits are, at most, a multiple of the current inflight ingester requests, some queueing of requests occurs when an ingester's reactive limiter is full. This allows short bursts of requests to be tolerated while still keeping the number of inflight requests under control. The amount of blocking that is allowed is based on the `initial-rejection-factor` and `max-rejection-factor`, and requests only queue when the limiter is full.

### Request rejection

Ingesters support separate reactive limiters for push and read requests, since these requests may be overloaded by different resources at different times. When ingesters are heavily loaded, these limiters may be full, and when the load is sustained, queues might fill up as well. When this happens, requests may be rejected, with a preference for rejecting read requests before rejecting push requests.

## Configure Grafana Mimir reactive limiters

To enable Grafana Mimir ingester push and read reactive limiters, set `-ingester.push-reactive-limiter.enabled=true` and `-ingester.read-reactive-limiter.enabled=true`. You can enable the push and read reactive limiters independently. Enabling one of them does not require enabling the other one.

### Primary configuration

When enabled, reactive limiters come with some default configurations that work well for many workloads. First among these are configurations for the short and long response time windows. These track how changes in recent, short-term response times compare to long-term response times.

When requests are processed by ingesters, their response times are first aggregated in the short window. A quantile from these is regularly taken once the window is full and added to the long window. By default, the minimum duration of the short window is `1s`, the minimum number of responses that must be observed is `50`, and the aggregated response time quantile that's taken is `.9`. The long window defaults to `60` measurements, which are smoothed over time. These values can be changed via `short-window-min-duration`, `short-window-max-duration`, `short-window-min-samples`, `sample-quantile`, and `long-window`.

The inflight request limit has a default range of `2` to `200`, and an initial value of `20`. You can change these values via `min-inflight-limit`, `max-inflight-limit`, and `initial-inflight-limit`. Additionally, the inflight limit only increases to a multiple of the current inflight requests, which defaults to `5.0`. You can change this value via `max-limit-factor`.

### Additional configurations

While response times are the primary mechanism for detecting overload, a secondary mechanism is tracking the correlation between inflight requests and throughput. The default size of this correlation window is `50` samples, which you can change via `correlation-window`.

The amount of queueing that is allowed when a limiter is full is based on the inflight request limit. By default, requests begin to be rejected instead of queued when the queue reaches `2` times the inflight limit, and all requests are rejected when the queue reaches `3` times the inflight limit. You can configure these settings via `initial-rejection-factor` and `max-rejection-factor`.

Rejection rates, which are based on recent limiter throughput statistics, are computed at `1s` intervals by default for all limiters. You can adjust this setting via `-ingester.rejection-prioritizer.calibration-interval`.

## Grafana Mimir reactive limiter metrics

Grafana Mimir ingester reactive limiters add the following metrics. These metrics aren't part of any API guarantee, and you can change them at any time:

- `cortex_ingester_reactive_limiter_inflight_requests`: Gauge showing the current number of requests that are inflight within the reactive limiter. It contains the `request_type` label, which is either `push` or `read`.
- `cortex_ingester_reactive_limiter_inflight_limit`: Gauge showing the current inflight request limit. It contains the `request_type` label, which is either `push` or `read`.
- `cortex_ingester_reactive_limiter_blocked_requests`: Gauge showing the current number of requests that are queued waiting on the limiter. It contains the `request_type` label, which is either `push` or `read`.
- `cortex_ingester_rejection_rate`: Gauge showing the current rate that new requests are rejected at.
