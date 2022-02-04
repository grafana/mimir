---
title: "Query Step Alignment analysis"
description: ""
weight: 100
---

# Query Step Alignment analysis

This tools allow to parse the query stats log messages of query-frontends to
understand the impact toggling query-step-alignment.

## Usage

- This requires a working Loki and logcli setup for the Mimir cluster

```
$ logcli query '{namespace="mimir", container="query-frontend"} |="path=/api/prom/api/v1/query_range" | logfmt' --limit 1000000 --output jsonl | go run main.go
time-frame:                  2022-01-27 09:35:52.108468366 +0000 UTC to 2022-01-27 10:35:51.62229847 +0000 UTC
total queries:                  192338
cachable-today:                  86232  44.83%
cachable-without-step-align:     85372  44.39%
step-aligned:                   168402  87.56%
```
