---
aliases:
  - ../../operators-guide/using-exemplars/about-exemplars/
  - ../../operators-guide/use-exemplars/about-exemplars/
description:
  Learn about using exemplars in Grafana Mimir to identify high cardinality
  in time series events.
menuTitle: About exemplars
title: About Grafana Mimir exemplars
weight: 10
---

# About Grafana Mimir exemplars

An exemplar is a specific trace representative of a repeated pattern of data in a given time interval. It helps you identify higher cardinality metadata from specific events within time series data. To learn more about exemplars and how they can help you isolate and troubleshoot problems with your systems, see [Introduction to exemplars](/docs/grafana/latest/basics/exemplars/).

Grafana Mimir includes the ability to store exemplars in-memory. Exemplar storage in Grafana Mimir is implemented similarly to how it is in Prometheus. Exemplars are stored as a fixed size circular buffer that stores exemplars in memory for all series.

The [limits]({{< relref "../../configure/configuration-parameters#limits" >}}) property can be used to control the size of the circular buffer by the number of exemplars. For reference, an exemplar with just a `traceID=<jaeger-trace-id>` uses roughly 100 bytes of memory via the in-memory exemplar storage. If the exemplar storage is enabled, Grafana Mimir will also append the exemplars to WAL for local persistence (for WAL duration).
