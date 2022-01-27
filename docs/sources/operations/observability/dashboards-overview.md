+++
title = "Dashboards overview"
weight = 1
+++

# Grafana Mimir dashboards overview

Grafana Mimir provides the following production-ready dashboards.

## Mimir / Writes

This dashboard shows various health metrics for the write path.
It includes object storage metrics for operations triggered by the read path.
It is broken into sections for each service on the write path,
and organized by the order in which the write request flows.

## Mimir / Writes Resources

This dashboard shows CPU, memory, disk and other resources utilization metrics.
It is broken into sections for each service on the write path,
and organized by the order in which the write request flows.

_Requires Kubernetes resources metrics._

## Mimir / Writes Networking

This dashboard shows receive/transmit bandwidth, inflight requests and TCP connections.
It is broken into sections for each service on the write path,
and organized by the order in which the write request flows.

_Requires Kubernetes networking metrics._

## Mimir / Reads

This dashboard shows health metrics for the read path.
It includes object storage metrics for operations triggered by the read path.
It is broken into sections for each service on the read path,
and organized by the order in which the read request flows.

## Mimir / Reads Resources

This dashboard shows CPU, memory, disk and other resources utilization metrics.
It is broken into sections for each service on the read path,
and organized by the order in which the read request flows.

_Requires Kubernetes resources metrics._

## Mimir / Reads Networking

This dashboard shows receive/transmit bandwidth, inflight requests and TCP connections.
It is broken into sections for each service on the read path,
and organized by the order in which the read request flows.

_Requires Kubernetes networking metrics._

## Mimir / Queries

This dashboard shows a drill-down into queries execution.

## Mimir / Compactor

This dashboard shows health and activity metrics for the compactor.
It includes object storage metrics for operations triggered by the compactor.

## Mimir / Compactor Resources

This dashboard shows CPU, memory, disk and networking metrics for the compactor.

_Requires Kubernetes resources metrics._

## Mimir / Ruler

This dashboard shows health and activity metrics for the ruler.
It includes object storage metrics for operations triggered by the ruler.

## Mimir / Alertmanager

This dashboard shows health and activity metrics for the alermanager.
It includes object storage metrics for operations triggered by the ruler.

## Mimir / Alertmanager Resources

This dashboard shows CPU, memory, disk and networking metrics for the alertmanager.

_Requires Kubernetes resources metrics._

## Mimir / Tenants

This dashboard shows various metrics for the selected tenant/user.

## Mimir / Top Tenants

This dashboard shows the top tenants based on multiple selection criterias.

## Mimir / Object Store

This dashboard shows an overview on all the activity / operations on the object storage,
run by any Mimir service.

## Mimir / Rollout progress

This dashboard shows the progress of a rollout across Mimir cluster.
It also shows some key metrics to monitor during rollouts, like failures rate and latency.

## Mimir / Slow Queries

This dashboard shows details about the slowest queries for a given time range.
It also allows to filter by a specific tenant/user.
If [Grafana Tempo](https://grafana.com/oss/tempo/) tracing is enabled, it also displays a link to the trace of each query.

_Requires Loki to fetch detailed query statistics from logs._

## Mimir / Config

This dashboard shows details about the runtime config currently loaded by each Mimir instance/replica.

## Mimir / Overrides

This dashboard shows global limits and per-tenant overrides.

## Mimir / Scaling

This dashboards shows any services which are not scaled correctly.
