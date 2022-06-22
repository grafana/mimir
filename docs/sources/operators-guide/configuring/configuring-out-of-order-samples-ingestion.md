---
title: "Configuring out-of-order samples ingestion"
menuTitle: "Configuring out-of-order samples ingestion"
description: "Learn how to configure Grafana Mimir to handle out-of-order samples ingestion."
weight: 120 
---

# Configuring out-of-order samples ingestion

By default Grafana Mimir will only accept new samples that are consequent to the existing ingested samples for a known series. Those that are older than the last sample that was ingested are called out-of-order samples.

If you happen to have out-of-order samples due to the nature of your architecture or the system that is being observed, then you can configure Grafana Mimir to set an allowance threshold for how old samples can be ingested.

### Configuration

To configure Grafana Mimir to accept out-of-order samples, perform the following steps:

1. Enable runtime configuration, for more information check [About runtime configuration]({{< relref "about-runtime-configuration.md" >}})
1. TBD: Give example how to configure flag. 