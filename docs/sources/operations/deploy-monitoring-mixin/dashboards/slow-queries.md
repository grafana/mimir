---
title: "Slow queries"
weight: 170
---

# Slow queries

This dashboard shows details about the slowest queries for a given time range.
It also allows to filter by a specific tenant/user.
If [Grafana Tempo](https://grafana.com/oss/tempo/) tracing is enabled, it also displays a link to the trace of each query.

Requires Loki to fetch detailed query statistics from logs.
