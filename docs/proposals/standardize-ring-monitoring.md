---
title: "0002: Standardize ring monitoring"
description: "Standardize ring monitoring"
draft: true
---

# 0002: Standardize ring monitoring

**Author:** @bubu11e

**Date:** 11/2023

**Sponsor(s):**

**Type:** Feature

**Status:** Draft

**Related issues/PRs:**

---

## Background

Mimir is using rings to ensure processes of sharding and replication. The following rings are used inside mimir:

- distributor
- ingester
- compactors
- store-gateway
- ruler (optional)
- alertmanager (optional)
- query-scheduler (optional)

Community based monitoring is currently ensured by three alerting rules:

- [MimirIngesterUnhealthy](https://github.com/grafana/mimir/blob/main/operations/mimir-mixin-compiled-baremetal/alerts.yaml#L4)
- [MimirAlertmanagerRingCheckFailing](https://github.com/grafana/mimir/blob/main/operations/mimir-mixin-compiled-baremetal/alerts.yaml#L534)
- [MimirRulerFailedRingCheck](https://github.com/grafana/mimir/blob/main/operations/mimir-mixin-compiled-baremetal/alerts.yaml#L441)

## Problem Statement

The previsoulsy mentionned rules does not cover monitoring of all the rings available in Mimir.

Furthermore their implementation differs:

- MimirIngesterUnhealthy alerting rule is based on metric `cortex_ring_members`available for all rings in the cluster

  - Probe does not take into account availability zones
  - Currently only focused on ingester ring
  - Does not indicate if the ring is broken or not but only if unhealthy instances have been found in it which could lead to unwanted alerts inside large cluster.

- MimirAlertmanagerRingCheckFailing alerting rule is based on metric `cortex_alertmanager_ring_check_errors_total` only available for alertmanager

  - Only apply to alertmanager ring
  - Expose ring usability
  - Does not alert about an unused broken ring.
  - This method needs to be reimplemented for every ring

- MimirRulerFailedRingCheck alerting rule is based on metric `cortex_ruler_ring_check_errors_total` only available for ruler
  - Only apply to ruler ring
  - Give an interesting point a view about the usability of the ring.
  - Does not alert about an unused broken ring.
  - This method needs to be reimplemented for every ring

## Goals

- Monitor all rings used inside Mimir software
- Give both a view of the state of the ring and of its usability
- Standardize monitoring of rings
- Automatically adapt to presents rings are some of thems are optionals

## Non-Goals (optional)

- Improve monitoring of other dskit based tools(loki/tempo/pyroscope) even if it could be a nice addition.

## Proposals

### Proposal 0: Do nothing

Current solution has proven to be working. The most critical ring (a.k.a ingester) is monitored.

**Pros:**

- Nothing to be done

**Cons:**

- Not all mimir rings are monitored
- Not homogenous among all rings
- Alert does not taking into account availability zone
- No indication on the ring state, only if there are unhealthy nodes and only for the ingester ring

### Proposal 1: Make alert MimirIngesterUnhealthy more generic

Rename the probe `MimirIngesterUnhealthy` to `MimirRingUnhealthy` and change it's expression to

    min by (cluster, namespace, name) (cortex_ring_members{state="Unhealthy"}) > 0

**Pros:**

- Really easy to implement as all needed ressources are already available inside mimir.
- All rings are monitored.
- Monitoring is partially consistent among all rings inside the cluster (except for the two additional probes on rings ruler and alertmanager)
- Monitoring adapts automatically to the rings activated inside mimir.

**Cons:**

- Probe are not taking into account zone-awareness
- Probe do not fully refelect the state of the ring (usable or not)
- Probe do not reflect the usage of the ring by the client except for the two additional probes on rings ruler and alertmanager

### Proposal 2: Copy ruler and alertmanager monitoring for other rings

Proposal 1 +

Implements metric cortex\_<ring_name>\_ring_check_errors_total for all rings in mimir.

Add as many alerting rules as there are rings inside mimir by using the following expression:

    sum by (cluster, namespace, job) (rate(cortex_<ring_name>_ring_check_errors_total[1m]))

**Pros:**

- All rings are monitored.
- Monitoring is consistent among all rings inside the cluster.

**Cons:**

- Probe only reflects usage of the ring by the client but not the ring' state.
- Hard to implement: there are as many implementations as there are rings.
- Potential additional ring won't take advantage of this work.
- Monitoring is not standard as it's implemented in various place in the code and is based on various metrics

### Proposal 3: Implement inside dskit/ring metrics to monitor rings.

Proposal 1 +

Like it's done for the `cortex_ring_members` metrics, implements two others metrics inside [dskit/ring](https://github.com/grafana/mimir/blob/main/vendor/github.com/grafana/dskit/ring/ring.go#L241):

- `cortex_ring_errors_total` which counts errors when trying to access the ring with a 'reason' label for every [error type](https://github.com/grafana/mimir/blob/main/vendor/github.com/grafana/dskit/ring/ring.go#L107).
- `cortex_ring_state` which stores a boolean representing the state of the ring as seen by the client with a label for various operations. State of the ring can be inferred by the use of [this function](https://github.com/grafana/mimir/blob/main/vendor/github.com/grafana/dskit/ring/ring.go#L474) with a test operation.

Add two additional alerting rules:

MimirRingErrors with the following expression

    sum by (cluster, namespace, job, name, reason) (rate(cortex_ring_errors_total[1m]))

MimirRingState with the following expression

    min by (cluster, namespace, name, operation) (cortex_ring_state{}) > 0

Finally remove dedicated alerting rules for ruler and alertmanager rings are they double the previous work.

**Pros:**

- All rings are monitored.
- Monitoring is consistent among all rings inside the cluster.
- This work can profite others dskit based software like tempo/loki/pyroscope
- Automatically adapt to the number of rings used inside the cluster
- Take into account zone-awareness

**Cons:**

## Other Notes
