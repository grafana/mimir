---
title: "(Optional) Ruler"
description: ""
weight: 20
---

# (Optional) Ruler

The ruler is an optional component that executes PromQL expressions defined in recording and alerting rules.
Each tenant has their own set of recording and alerting rules.
Each tenant can group recording and alerting rules into namespaces.

The ruler supports multi-tenancy and horizontal scalability.

## Sharding

To achieve horizontal scalability, the ruler shards the execution of rules by rule groups.
The ruler replicas use the [hash ring]({{<relref "./about-the-hash-ring.md" >}}) stored in the [KV store]({{<relref "./about-the-key-value-store.md" >}}) to divide up the work of executing rules.

## HTTP configuration API

The ruler HTTP configuration API allows tenants to create, update, and delete rule groups.
For a full list of endpoints and example requests, refer to [ruler]({{<relref "../reference-http-api/_index.md#ruler" >}}).

## State

The ruler stores ruler state in the storage backend configured with `-ruler-storage.backend`.
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

The ruler would looks for tenant rules in the `/tmp/rules/<TENANT ID>` directory.
The ruler expects rule files to be in the [Prometheus format](https://prometheus.io/docs/prometheus/latest/configuration/recording_rules/#recording-rules).
