---
aliases:
  - ../configuring/configuring-ingester-circuit-breakers/
description: Learn how to configure ingester circuit breakers.
menuTitle: Configure ingester circuit breakers
title: Configure Grafana Mimir ingester circuit breakers
---

# Configure Grafana Mimir ingester circuit breakers

## How do Grafana Mimir Ingester circuit breakers work?

A request to a resource protected by a circuit breaker follows these steps:

- The request tries to acquire a circuit breaker permit.
- If the circuit breaker is open, no permit is acquired, and the request fails with a _circuit breaker open error_.
- Otherwise, the request acquires a circuit breaker permit and runs.
  - If the request meets the circuit breaker's failure condition, the circuit breaker records a failure.
  - Otherwise, the circuit breaker records a success.

Depending on configurable frequencies of successes and failures, the circuit breaker transits from one state to another.

Grafana Mimir ingester circuit breakers are an experimental feature.
They independently protect Mimir's write and read paths from slow push and read requests.
More precisely, Grafana Mimir ingesters distinguish between _push requests circuit breakers_ and _read requests circuit breakers_.

### Push requests circuit breakers

A push request meets the push requests circuit breaker failure condition if its duration is longer than the configurable maximum push request duration.

### Read requests circuit breakers

Read requests circuit breakers follow these conditions:

- in order to protect the write path as much as possible, ingesters do not allow read push requests if their push circuit breakers are open.
  This means that before an ingester tries to acquire a read circuit breaker permit, it first checks if its push circuit breaker is open.
  If it's open, a circuit breaker open error is returned.
  Otherwise, the ingester tries to acquire a read requests circuit breaker permit.
- a read request that acquired a read requests circuit breaker permit meets the read requests circuit breaker failure condition if its duration is longer than the configurable read request maximum duration.

## Grafana Mimir Ingester circuit breakers configuration

You can configure Grafana Mimir ingester circuit breakers with the following options.

- `-ingester.push-circuit-breaker.enabled`: Enable circuit breaking when making push requests to ingesters.

- `-ingester.read-circuit-breaker.enabled`: Enable circuit breaking when making read requests to ingesters.

- `-ingester.push-circuit-breaker.thresholding-period`: Moving window of time that the percentage of failed requests is computed over for push circuit breakers.

- `-ingester.read-circuit-breaker.thresholding-period`: Moving window of time that the percentage of failed requests is computed over for read circuit breakers.

- `-ingester.push-circuit-breaker.failure-execution-threshold`: How many push requests must have been executed within the thresholding period for a push circuit breaker to be eligible to open for the rate of failures?

- `-ingester.read-circuit-breaker.failure-execution-threshold`: How many read requests must have been executed within the thresholding period for a read circuit breaker to be eligible to open for the rate of failures?

- `-ingester.push-circuit-breaker.failure-threshold-percentage`: Maximum percentage of push requests that can fail over period before a push circuit breaker opens.

- `-ingester.read-circuit-breaker.failure-threshold-percentage`: Maximum percentage of read requests that can fail over period before a read circuit breaker opens.

- `-ingester.push-circuit-breaker.cooldown-period`: How long a push circuit breaker will stay in the open state before allowing additional push requests?

- `-ingester.read-circuit-breaker.cooldown-period`: How long a read circuit breaker will stay in the open state before allowing additional read requests?

- `-ingester.push-circuit-breaker.initial-delay`: How long a push circuit breaker waits to become active after an activation request. During this time, neither failures nor successes are counted.

- `-ingester.read-circuit-breaker.initial-delay`: How long a read circuit breaker should wait between an activation request and becoming effectively active? During that time both failures and successes are not counted.

- `-ingester.push-circuit-breaker.request-timeout`: The maximum duration of a push request before it triggers a timeout. This configuration is used for push circuit breakers only, and the corresponding timeouts are not reported as errors.

- `-ingester.read-circuit-breaker.request-timeout`: The maximum duration of a read request before it triggers a timeout. This configuration is used for read circuit breakers only, and the corresponding timeouts are not reported as errors.

## Grafana Mimir Ingester circuit breakers metrics

Grafana Mimir ingester circuit breakers contain the following metrics:

- `cortex_ingester_circuit_breaker_transitions_total`: Counter showing the number of times a circuit breaker enters a state. It contains the labels state, with possible values of `closed`, `open`, and `half-open`, and the `request_type` setting , with possible values of `push` and `read`.

- `cortex_ingester_circuit_breaker_results_total`: Counter showing the results of executing requests via a circuit breaker. It contains lables result, with possible values of `success`, `error`, and `circuit_breaker_open`,  and the `request_type` setting, with possible values of `push` and `read`.

- `cortex_ingester_circuit_breaker_request_timeouts_total`: Counter showing the number of times the circuit breaker records a request that reaches timeout. It contains the `request_type` label, with possible values of `push` and `read`.

- `cortex_ingester_circuit_breaker_current_state`: Gauge set to `1` when the circuit breaker is in a state corresponding to the label name. It contains the labels state, with possible values of `closed`, `open`, and `half-open`, and the `request_type` setting with possible values of `push` and `read`.
