---
description: Learn about the network ports that Grafana Mimir uses
menuTitle: Network ports
title: About Grafana Mimir network ports
weight: 25
---

# About Grafana Mimir network ports

Grafana Mimir uses various network ports to facilitate communication between its internal components, external services like Prometheus and Grafana, and for overall cluster operation. Proper port configuration is crucial for setting up your Mimir cluster, configuring firewalls, and ensuring secure communication between Mimir components and integrated tools.

The ports required to run Grafana Mimir can vary slightly depending on your deployment mode and whether you're using additional components like Grafana or a load balancer.

The following table shows the default ports that are fundamental to operating Mimir, whether in a monolithic or distributed setup. You can update these values in your Mimir configuration.

| Port | Function                    | Related components   | Description                                                                                                                                                                                                                                                                                                                 |
| :--- | :-------------------------- | :------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 8080 | HTTP API / remote write     | All Mimir components | This is the main entry point for Prometheus to remote-write metrics to Mimir through the Distributor and for Grafana and Prometheus to query data through the Querier or Query-frontend. This port is not typically exposed, as Grafana Mimir generally runs behind an Nginx proxy, the GEM gateway, or Kubernetes ingress. |
| 9095 | Internal gRPC communication | All Mimir components | Used for high-performance communication between different Mimir components, such as Distributor to Ingester, or Querier to Ingester. This communication is essential for distributed deployments.                                                                                                                           |
| 7946 | Memberlist / Gossip         | All Mimir components | Used for service discovery and maintaining the consistent hash ring that allows Mimir components to find and communicate with each other. This process is critical for high availability and scaling.                                                                                                                       |
