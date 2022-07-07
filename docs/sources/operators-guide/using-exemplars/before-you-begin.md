---
aliases:
  - /docs/mimir/latest/operators-guide/using-exemplars/before-you-begin/
description: Refer to this checklist before you begin using exemplars in Grafana Mimir.
menuTitle: Before you begin
title: Before you begin using exemplars with Grafana Mimir
weight: 20
---

# Before you begin using exemplars with Grafana Mimir

Follow the checklist to ensure that your application is generating metrics, traces, and exemplars.

- Verify that your application is using the official Prometheus client libraries.
- Ensure that the client library you choose is emitting metrics in [OpenMetrics](https://openmetrics.io/) format by referencing its documentation. For the Prometheus Go client library, for example, this requires you to set `EnableOpenMetrics` to `true`. For the Java library, follow [its instructions](https://github.com/prometheus/client_java#exemplars) on setting the proper header format.
- Obtain the trace ID for the current request and include the trace ID in calls to emit metrics.
  - For histograms, use the `ObserveWithExemplar` method to emit the trace ID along with a value for the histogram. These functions are from the Go library but you can find similar functions in the other libraries.
  - For counters, use the `AddWithExemplar` method to emit the trace ID along with a counter increment.
- Verify that metrics are being generated with exemplars by running the following command in a shell: `curl -H "Accept: application/openmetrics-text" http://<your application>/metrics | grep -i "traceid"`.
- Configure your Prometheus server or Grafana Agent to store and send exemplars.
  - To configure Grafana Agent to send exemplars:
    1. Confirm that the Agent is scraping exemplars by verifying that the `prometheus_remote_storage_exemplars_total` metric is a non-zero value.
    1. Add the option `send_exemplars: true` under the `remote_write` configuration block in the Grafana Agent configuration file.
  - To configure a Prometheus server to send exemplars:
    1. Run Prometheus with the `--enable-feature=exemplar-storage` flag.
    1. Confirm that Prometheus is scraping exemplars by verifying that the `prometheus_remote_storage_exemplars_total` metric is a non-zero value.
    1. Add the option `send_exemplars: true` under the `remote_write` configuration block in the Prometheus configuration file.

See also:

- [Storing exemplars in Grafana Mimir]({{< relref "storing-exemplars.md" >}})
- [Viewing exemplar data]({{< relref "viewing-exemplar-data.md" >}})
- [TNS demo](https://github.com/grafana/tns)
