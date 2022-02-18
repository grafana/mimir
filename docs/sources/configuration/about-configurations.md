---
title: "About configurations"
description: ""
weight: 10
---

# About configurations

Grafana Mimir is configured via command-line-interface (CLI) flags or a configuration file. Every parameter that can be set in the configuration file can also be set via a corresponding command-line flag. If you specify both command-line flags and YAML configuration parameters, the command-line flags take precedence over values in a YAML file.

To view the most common flags needed to get started with Grafana Mimir, run `mimir -help`. To view all available command line flags, run `mimir -help-all`.

This configuration loads at startup and cannot be modified at runtime. However, Grafana Mimir does have a second configuration file, known as the runtime configuration, that is dynamically reloaded. For more information, see Runtime configuration.

To see the current configuration state of any component, use the [`/config`](../../reference-http-api/#configuration) HTTP API endpoint.

## Operational considerations

It is best to specify your configuration via the configuration file rather than CLI flags.

Use a single configuration file, and either pass it to all replicas of Grafana Mimir (when running multiple single-process Mimir replicas) or to all components of Grafana Mimir (when running Grafana Mimir as microservices). When running Grafana Mimir on Kubernetes, you can achieve this by storing the configuration file in a [ConfigMap](https://kubernetes.io/docs/concepts/configuration/configmap/) and mounting it in each Grafana Mimir container.

This recommendation helps to avoid a common misconfiguration pitfall: while certain configuration parameters might look like they’re only needed by one type of component, they might in fact be used by multiple components. For example, the `-ingester.ring.replication-factor` flag is not only required by ingesters, but also by distributors, queriers, and rulers.

By using a single configuration file, you ensure that each component gets all the configuration it needs without needing to track which parameter belongs to which component.
There is no harm in passing a configuration that is specific to one component, such as an ingester, to another component, such as a querier. It is simply ignored.

If needed, advanced users can use CLI flags to override specific values on a particular Grafana Mimir component or replica. This can be helpful if you want to change a parameter that is specific to a certain component, without having to do a full restart of all other components. The most common case for CLI flags is to use the `-target` flag to run Grafana Mimir as microservices. All Grafana Mimir components share one configuration file, but are made to behave as a particular component by setting different `-target` command-line values, for example `-target=ingester` or `-target=querier`.

## Configuration file

To specify which configuration file to load, use the `-config.file` command-line option.

The configuration file is written in [YAML](https://en.wikipedia.org/wiki/YAML) and follows the structure defined below. Parameters shown in brackets are optional.
