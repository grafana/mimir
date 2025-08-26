---
aliases:
  - ../../operators-guide/architecture/deployment-modes/
description: You can deploy Grafana Mimir in either monolithic mode or microservices mode.
menuTitle: Deployment modes
title: Grafana Mimir deployment modes
weight: 20
---

<!-- Note: This topic is mounted in the GEM documentation. Ensure that all updates are also applicable to GEM. -->

# Grafana Mimir deployment modes

Grafana Mimir offers two deployment modes to accommodate different operational requirements and scale needs. Choose the deployment mode that best fits your use case:

- Monolithic mode: Run all components in a single process for simple deployments.
- Microservices mode: Deploy components separately for maximum scalability and flexibility.

Configure the deployment mode using the `-target` parameter, which you can set via CLI flag or YAML configuration.

## About monolithic mode

Monolithic mode runs all required components in a single process and is the default deployment mode. You enable monolithic mode by setting `-target=all`.

This mode is ideal when you want to:

- Get started quickly with Mimir
- Run Mimir in a development environment
- Deploy with minimal operational complexity
- Scale all components together

To see the complete list of components that run in monolithic mode, use the `-modules` flag:

```bash
./mimir -modules
```

[//]: # "Diagram source at https://docs.google.com/presentation/d/1LemaTVqa4Lf_tpql060vVoDGXrthp-Pie_SQL7qwHjc/edit#slide=id.g11694eaa76e_0_0"

![Mimir's monolithic mode](monolithic-mode.svg)

### Scale monolithic mode

You can horizontally scale monolithic mode by deploying multiple Mimir binaries with `-target=all`. This approach provides high availability and increased scale without the configuration complexity of [microservices mode](#about-microservices-mode).

[//]: # "Diagram source at https://docs.google.com/presentation/d/1LemaTVqa4Lf_tpql060vVoDGXrthp-Pie_SQL7qwHjc/edit#slide=id.g11658e7e4c6_1_20"

![Mimir's horizontally scaled monolithic mode](scaled-monolithic-mode.svg)

{{< admonition type="note" >}}
Because monolithic mode requires scaling all Grafana Mimir components together, this deployment mode isn't recommended for large-scale deployments.
{{< /admonition >}}

## About microservices mode

Microservices mode deploys each component in separate processes, enabling independent scaling and creating granular failure domains. This deployment mode is recommended for production environments that require high availability and flexible scaling.

In microservices mode, each Grafana Mimir process is invoked with its `-target` parameter set to a specific Grafana Mimir component (for example, `-target=ingester` or `-target=distributor`). To get a working Grafana Mimir instance, you must deploy every required component. For more information about each of the Grafana Mimir components, refer to [Grafana Mimir advanced architecture](https://grafana.com/docs/mimir/<GRAFANA_VERSION>/references/architecture/).

To deploy Grafana Mimir in microservices mode, we recommend that you use [Kubernetes](https://kubernetes.io/) and the [mimir-distributed Helm chart](https://github.com/grafana/mimir/tree/main/operations/helm/charts/mimir-distributed).

[//]: # "Diagram source at https://docs.google.com/presentation/d/1LemaTVqa4Lf_tpql060vVoDGXrthp-Pie_SQL7qwHjc/edit#slide=id.g11658e7e4c6_1_53"

![Mimir's microservices mode](microservices-mode.svg)

## Read-Write mode

{{< admonition type="warning" >}}
Read-write deployment mode is not supported in Grafana Mimir versions 3.0 and later. Use [monolithic mode](#about-monolithic-mode) or [microservices mode](#about-microservices-mode) instead.
{{< /admonition >}}

The read-write mode provides an alternative to monolithic and microservices modes.

In read-write mode, components are grouped into three services to ease the operational overhead whilst still allowing scale to be tuned separately on the read and write paths. The services group the components as follows:

- read
  - query-frontend
  - querier
- backend
  - store-gateway
  - compactor
  - ruler
  - alertmanager
  - query-scheduler
  - overrides-exporter
- write
  - distributor
  - ingester

Similar to the other modes, each Grafana Mimir process is invoked with its `-target` parameter set to the specific service: `-target=read`, `-target=write`, or `-target=backend`.

[//]: # "Diagram source at https://docs.google.com/drawings/d/18Qfl-H9On9zi2IRVX-rLawbpQPRcMcI0xh5uwyUjlak"

![Mimir's read-write deployment mode](read-write-mode.svg)

Read-write mode is only available in [Jsonnet](../../../set-up/jsonnet/deploy/).
