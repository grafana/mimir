---
aliases:
  - /docs/mimir/latest/operators-guide/configuring/setting-helm-ha-deduplication-consul/
description: Learn how to configure Grafana Mimir Helm Chart to handle HA Prometheus server deduplication with Consul.
menuTitle: Configuring high-availability deduplication with Consul
title: Configuring Grafana Mimir Helm Chart for high-availability deduplication with Consul
weight: 70
---

# Configuring Grafana Mimir Helm Chart for high-availability deduplication with Consul

Grafana Mimir can deduplicate data from high-availability (HA) Prometheus setup. In this guide, you will see how to configure
the deduplication for Grafana Mimir helm deployment using external Consul.

## Before you begin

You need to have a Grafana Mimir installed.

Refer to [Configuring High Availability]({{< relref "../configuring-high-availability-deduplication.md" >}}) documents
for high level description on the concept how Mimir deduplicate incoming HA samples.

You also have to configure HA for Prometheus or Grafana Agent. Last, you need a KV store. In this guide you will
use Consul. You will be guided on the setup if you haven't had one.

## Configure Prometheus or Grafana Agent to send HA external labels

Configure Prometheus or Grafana Agent HA setup by setting label called `cluster` and `__replica__`. These two labels
name are default labels for HA setup in Grafana Mimir. If you want to change the labels, make sure to change it in Mimir
too to make Grafana Mimir and Prometheus/Grafana Agent config matches with each other, otherwise HA deduplication
wouldn't work. `cluster` label value must be same across replica belong to the same cluster. `__replica__` must be
unique across different replica in the cluster.

```yaml
global:
  scrape_interval: 1m
  scrape_timeout: 10s
  evaluation_interval: 1m
  external_labels:
    __replica__: replica-1
    cluster: my-prometheus
```

Reload or restart Prometheus or Grafana Agent after updating the configuration.

## Install Consul using Helm

Install Consul in Kubernetes using
[Consul helm chart](https://github.com/hashicorp/consul-k8s/tree/main/charts/consul). Follow the documentation to get
the chart and installing a Consul release. Put a note on the Consul endpoint, because you will need it for Mimir
configuration.

## Setup Mimir HA config

You can set Mimir HA deduplication configuration in global level or tenant level.

### HA Deduplication Global

You need a Mimir setup installed by Helm. Add the following configuration to your `values.yaml` to configure HA
deduplication globally.

```yaml
# other configurations above
mimir:
  structuredConfig:
    limits:
      accept_ha_samples: true
      # The following two configs must match with external_labels config in Prometheus
      # The config values below are the default and can be removed if you don't want to override to a new value
      ha_cluster_label: cluster
      ha_replica_label: __replica__
    distributor:
      ha_tracker:
        enable_ha_tracker: true
        kvstore:
          store: consul
          consul:
            host: <consul-endpoint> # example: http://mimir-host:8500
```

Upgrade the Mimir's helm release using the above configuration.

```bash
 helm -n <mimir-namespace> upgrade --install mimir grafana/mimir-distributed -f values.yaml
```

### HA Deduplication per tenant

Add the following configuration below the `values.yaml` file to configure HA deduplication per-tenant

```yaml
runtimeConfig:
  overrides:
    <tenant-id>: # put real tenant ID here
      accept_ha_samples: true
      ha_cluster_label: cluster
      ha_replica_label: __replica__
```

Upgrade the Mimir's helm release using the above configuration.

```bash
 helm -n <mimir-namespace> upgrade --install mimir grafana/mimir-distributed -f values.yaml
```

## Verifying deduplication

After Consul, Prometheus and Mimir running. Port forward Mimir distributor service. The argument after port-forward must
match your Mimir's distributor name.

```bash
kubectl port-forward service/mymimir-distributor 8080:8080
```

Open `http://localhost:8080/distributor/ha_tracker` in a browser. You should see the output similar like the following.
If the table is empty, it means there is something wrong with the configuration.

![HA Tracker Status](ha-tracker-status.png)

Go to Grafana explore page and select Mimir datasource. Then execute the following query: `up`. In the Options drop down,
select Format = Table. In the result you can see the several time series with different labels.

![Verify Deduplication](verify-deduplication.png)

The most important thing is you will not find `__replica__` label (or any label that you set in `ha_replica_label`
config) anymore. This means you have configured the deduplication successfully.
