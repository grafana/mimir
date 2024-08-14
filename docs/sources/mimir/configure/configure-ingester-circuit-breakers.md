---
aliases:
  - ../configuring/configuring-ingester-circuit-breakers/
description: Learn how to configure ingester circuit breakers.
menuTitle: Configure ingester circuit breakers
title: Configure Grafana Mimir ingester circuit breakers
---

# Configure Grafana Mimir ingester circuit breakers

## Background

Use circuit breakers to prevent an application from repeatedly trying to run an operation that is likely to fail.
A circuit breaker monitors the number of recent failures and then uses this information to decide whether to allow a new operation to proceed, or to simply return an exception immediately.
In the case of a failing operation, a circuit breaker allows an application to proceed, without waiting for you to resolve the failure cause.
Because the failing operation is immediately rejected, the application doesn't retry it. This reduces the application's CPU usage.

The circuit breaker pattern operates in three states: `closed`, `open`, and `half-open`.

- In the `closed` state, a circuit breaker operates normally, forwarding all requests to the application it protects.
- In the `open` state, the circuit breaker immediately stops forwarding requests to the failing application, effectively isolating it.
- After a specified timeout period in the `open` state, the circuit breaker transitions to the `half-open` state, where it forwards a limited number of trial requests to the application and monitors their execution.
  Successful trial requests indicate application recovery, and the circuit breaker transitions back to the `closed` state.
  Failing trial requests indicate that the issues persist, and the circuit breaker transitions back to the `open` state.

## How do Grafana Mimir Ingester circuit breakers work?

A request to a resource protected by a circuit breaker follows these steps:

1. The request tries to acquire a circuit breaker permit.
1. If the circuit breaker is open, no permit is acquired, and the request fails with a _circuit breaker open error_.
1. Otherwise, the request acquires a circuit breaker permit and runs.
1. If the request meets the circuit breaker's failure condition, the circuit breaker records a failure.
   Otherwise, the circuit breaker records a success.

Depending on the configured frequencies of successes and failures, the circuit breaker transits from one state to another.

{{% admonition type="note" %}}Grafana Mimir ingester circuit breakers are an experimental feature.
They independently protect Mimir's write and read paths from slow requests.
For the time being they don't protect ingesters from other issues.
{{% /admonition %}}

More precisely, Grafana Mimir ingesters distinguish between _push requests circuit breakers_ and _read requests circuit breakers_, and they can be configured independently.
It is possible to configure the maximum allowed duration of both push and read requests, as well as the highest frequency of the slow requests before the circuit breakers open and start protecting the ingesters.

### Push requests circuit breakers

A push request meets the push requests circuit breaker failure condition if its duration is longer than the configured maximum push request duration.

### Read requests circuit breakers

Read requests circuit breakers follow these conditions:

- in order to protect the write path as much as possible, ingesters do not allow read push requests if their push circuit breakers are open.
  This means that before an ingester tries to acquire a read circuit breaker permit, it first checks if its push circuit breaker is open.
  If it iss open, a circuit breaker open error is returned.
  Otherwise, the ingester tries to acquire a read requests circuit breaker permit.
- a read request that acquired a read requests circuit breaker permit meets the read requests circuit breaker failure condition if its duration is longer than the configured read request maximum duration.

## Grafana Mimir Ingester circuit breakers configuration

You can enable Grafana Mimir ingester push and read circuit breaker by respectively setting `-ingester.push-circuit-breaker.enabled=true` and `-ingester.read-circuit-breaker.enabled=true`.
You can enable the circuit breakers independently, i.e., enabling one of them does not require enabling the other one.

When enabled, push and read circuit breakers come with some default configurations.
For example, the percentage of failing requests is computed over the moving window of 1 minute (`1m`).
You can change these configurations by setting `-ingester.push-circuit-breaker.thresholding-period` or `-ingester.read-circuit-breaker.thresholding-period` to a desired value.

The default percentage of push and pull requests that can fail over the configured moving window before the corresponding circuit breaker opens is `10`.
You can set a different percentage via `-ingester.push-circuit-breaker.failure-threshold-percentage` and `-ingester.read-circuit-breaker.failure-threshold-percentage` flags.

Once a circuit breaker reaches the `open` state, it waits for 10 seconds (`10s`) before it transitions to the `half-open` state and allows trial requests.
You can change these waiting times via `-ingester.push-circuit-breaker.cooldown-period` and `-ingester.read-circuit-breaker.cooldown-period` flags.

Grafana Mimir ingester push and read circuit breakers come with one different default configuration: the maximum duration of a request before it triggers a timeout.
The default values are 2 seconds (`2s`) for push requests, and 30 seconds (`30s`) for read requests, and can be configured via `-ingester.push-circuit-breaker.request-timeout` and `-ingester.read-circuit-breaker.request-timeout`.
Circuit breakers use these timeouts only internally, and never report them as errors.

## Grafana Mimir Ingester circuit breakers metrics

Grafana Mimir ingester circuit breakers add several metrics, which aren't part of any API guarantee and can be changed at any time.

- `cortex_ingester_circuit_breaker_transitions_total`: Counter showing the number of times a circuit breaker enters a state. It contains the labels state, with possible values of `closed`, `open`, and `half-open`, and the `request_type` setting , with possible values of `push` and `read`.

- `cortex_ingester_circuit_breaker_results_total`: Counter showing the results of executing requests via a circuit breaker. It contains lables result, with possible values of `success`, `error`, and `circuit_breaker_open`, and the `request_type` setting, with possible values of `push` and `read`.

- `cortex_ingester_circuit_breaker_request_timeouts_total`: Counter showing the number of times the circuit breaker records a request that reaches timeout. It contains the `request_type` label, with possible values of `push` and `read`.

- `cortex_ingester_circuit_breaker_current_state`: Gauge set to `1` when the circuit breaker is in a state corresponding to the label name. It contains the labels state, with possible values of `closed`, `open`, and `half-open`, and the `request_type` setting with possible values of `push` and `read`.
