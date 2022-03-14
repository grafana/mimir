---
title: "Slow queries"
weight: 10
---

# Slow queries

The Slow queries dashboard shows details about the slowest queries for a given time range and enables you to filter results by a specific tenant.

If you enable [Grafana Tempo](https://grafana.com/oss/tempo/) tracing, the dashboard displays a link to the trace of each query.

This dashboard requires [Grafana Loki](https://grafana.com/oss/loki/) to fetch detailed query statistics from logs.
