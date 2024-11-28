---
description: Learn how to deploy Grafana Enterprise Metrics using the Helm chart.
menuTitle: Deploy GEM
title: Deploy Grafana Enterprise Metrics using the Helm chart
weight: 40
---

# Deploy Grafana Enterprise Metrics

Use the mimir-distributed Helm chart to deploy Grafana Enterprise Metrics (GEM) on Kubernetes. Here are the main differences in using the Helm chart for GEM, as compared to Grafana Mimir:

- GEM requires a valid license.
- Instead of using NGINX as the router of requests to internal components, GEM deploys its own enterprise gateway component that authenticates and routes requests.
- GEM has more mandatory and optional components, such as the Admin API and various proxies.

## Before you begin

- Follow the instructions and [Choose a name for your GEM cluster](https://grafana.com/docs/enterprise-metrics/<GEM_VERSION>/setup/#choose-a-name-for-your-gem-cluster).

  It is recommended to use the same name as the Helm release. For example, if the cluster name is `mygem`, you'd install the chart with `helm install mygem grafana/mimir-distributed`.

- Follow the instructions in [Get a GEM license](https://grafana.com/docs/enterprise-metrics/<GEM_VERSION>/setup/#get-a-gem-license) to acquire a license.

## Provide the license file

There are two options:

- Provide the license as a value for the `license.contents` Helm value.

  Either on the command line for the `helm` command as `--set-file 'license.contents=./license.jwt'` or by writing the contents into your custom values:

  ```yaml
  license:
    contents: "iyJhbGci..."
  ```

- Store the license in a Kubernetes [secret](https://kubernetes.io/docs/concepts/configuration/secret/).

  In this case, use the following custom values:

  ```yaml
  license:
    external: true
    secretName: <name-of-your-secret>
  ```

## Enable GEM in the configuration

Add the following value to your custom values:

```yaml
enterprise:
  enabled: true
```

If you want to use a different cluster name for the license and a different Helm release name on the command line, also set the following:

```yaml
mimir:
  structuredConfig:
    cluster_name: <cluster-name-in-license>
```
