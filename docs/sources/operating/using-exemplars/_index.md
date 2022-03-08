---
title: "Using exemplars with Grafana Mimir"
description: ""
weight: 40
---

# Using exemplars with Grafana Mimir

An exemplar is a specific trace representative of a repeated pattern of data in a given time interval. It helps you identify higher cardinality metadata from specific events within time series data. To learn more about exemplars and how they can help you isolate and troubleshoot problems with your systems, see [Introduction to exemplars](https://grafana.com/docs/grafana/latest/basics/exemplars/).

Grafana Mimir includes the ability to store exemplars in-memory. Exemplar storage in Grafana Mimir is implemented similarly to how it is in Prometheus. Exemplars are stored as a fixed size circular buffer that stores exemplars in memory for all series.

The [limits]({{< relref "../../configuring/reference-configuration-parameters.md#limits" >}}) property can be used to control the size of the circular buffer by the number of exemplars. For reference, an exemplar with just a `traceID=<jaeger-trace-id>` uses roughly 100 bytes of memory via the in-memory exemplar storage. If the exemplar storage is enabled, Grafana Mimir will also append the exemplars to WAL for local persistence (for WAL duration).

See also:

- [Before you begin]({{< relref "before-you-begin.md" >}})
- [Storing exemplars in Grafana Mimir]({{< relref "storing-exemplars.md" >}})
- [Viewing exemplar data]({{< relref "viewing-exemplar-data.md" >}})
