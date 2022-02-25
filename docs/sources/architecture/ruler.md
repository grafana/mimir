---
title: "(Optional) Ruler"
description: ""
weight: 20
---

# (Optional) Ruler

The ruler is an optional component that evaluates PromQL expressions defined in recording and alerting rules.
Each tenant has their own set of recording and alerting rules.
Each tenant can group recording and alerting rules into namespaces.

## Recording rules

The ruler evaluates the expressions in recording rules at regular intervals and writes the result back to ingesters.
The ruler has a built-in querier to evaluate the PromQL expressions and a built-in distributor so that it can write directly to ingesters.
Configuration of the built-in querier and distributor uses their respective configuration parameters.
For querier configuration parameters, refer to [querier]({{<relref "../configuration/reference-configuration-parameters.md#querier" >}}).
For distributor configuration parameters, refer to [distributor]({{<relref "../configuration/reference-configuration-parameters.md#distributor" >}}).

## Alerting rules

The ruler evaluates the expressions in alerting rules at regular intervals and if the result includes any series, the alert becomes active.
If an alerting rule has a defined `for` duration, it enters the "PENDING" state.
Once the alert has been active for the entire `for` duration, it enters the "FIRING" state.
The ruler notifies Alertmanagers of any "FIRING" alerts.
Configure the addresses of Alertmanagers with the `-ruler.alertmanager-url` flag.
The `-ruler.alertmanager-url` flag supports the DNS service discovery format.
For more information about DNS service discovery, refer to [Supported discovery modes]({{<relref "../configuration/about-grafana-mimir-arguments.md#supported-discovery-modes" >}}).

## Sharding

The ruler supports multi-tenancy and horizontal scalability.
To achieve horizontal scalability, the ruler shards the execution of rules by rule groups.
The ruler replicas form their own [hash ring]({{<relref "./about-the-hash-ring.md" >}}) stored in the [KV store]({{<relref "./about-the-key-value-store.md" >}}) to divide up the work of executing rules.

## HTTP configuration API

The ruler HTTP configuration API allows tenants to create, update, and delete rule groups.
For a full list of endpoints and example requests, refer to [ruler]({{<relref "../reference-http-api/_index.md#ruler" >}}).

## State

The ruler stores configured rules in the storage backend configured with `-ruler-storage.backend`.
Supported backends include:

- [Amazon S3](https://aws.amazon.com/s3): `-ruler-storage.backend=s3`
- [Google Cloud Storage](https://cloud.google.com/storage/): `-ruler-storage.backend=gcs`
- [Microsoft Azure Storage](https://azure.microsoft.com/en-us/services/storage/): `-ruler-storage.backend=azure`
- [OpenStack Swift](https://wiki.openstack.org/wiki/Swift): `-ruler-storage.backend=swift`
- [Local storage]({{<relref "#local-storage" >}}): `-ruler-storage.backend=local`

### Local storage

The `local` storage backend reads [Prometheus recording rules](https://prometheus.io/docs/prometheus/latest/configuration/recording_rules/) from the local filesystem.

> **Note:**
> Local storage is a readonly backend and it doesn't support the creation and deletion of rules through the [Configuration API]({{<relref "#http-configuration-api" >}}).

Local storage supports ruler sharding when all rulers have the same rules files.
In Kubernetes, mounting a [Kubernetes ConfigMap](https://kubernetes.io/docs/concepts/configuration/configmap/) into every ruler Pod would facilitate sharding.

With local storage defined as follows:

```
-ruler-storage.backend=local
-ruler-storage.local.directory=/tmp/rules
```

The ruler looks for tenant rules in the `/tmp/rules/<TENANT ID>` directory.
The ruler expects rule files to be in the [Prometheus format](https://prometheus.io/docs/prometheus/latest/configuration/recording_rules/#recording-rules).
