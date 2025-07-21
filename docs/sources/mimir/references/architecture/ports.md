---
description: Learn about the network ports that Grafana Mimir uses
menuTitle: Network ports
title: About Grafana Mimir network ports
weight: 25
---

<!-- Note: This topic is mounted in the GEM documentation. Ensure that all updates are also applicable to GEM. -->

# About Grafana Mimir network ports

Grafana Mimir uses various network ports to facilitate communication between its internal components, external services like Prometheus and Grafana, and for overall cluster operation. Proper port configuration is crucial for setting up your Mimir cluster, configuring firewalls, and ensuring secure communication between Mimir components and integrated tools.

The ports required to run Grafana Mimir can vary slightly depending on your deployment mode and whether you're using additional components like Grafana or a load balancer.

## Core Mimir ports

The following table shows the core Mimir ports.These ports are fundamental to operating Mimir, whether in a monolithic or distributed setup.

| Port | Function                    | Related components   | Description                                                                                                                                                                                                                    |
| :--- | :-------------------------- | :------------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 9009 | HTTP API / remote write     | Distributor, Querier | This is the main entry point for Prometheus to remote-write metrics to Mimir through the Distributor and for Grafana and Prometheus to query data through the Querier or Query-frontend. This is the primary user-facing port. |
| 9095 | Internal gRPC communication | All Mimir components | Used for high-performance communication between different Mimir components, such as Distributor to Ingester, or Querier to Ingester. This communication is essential for distributed deployments.                              |
| 7946 | Memberlist / Gossip         | All Mimir components | Used for service discovery and maintaining the consistent hash ring that allows Mimir components to find and communicate with each other. This process is critical for high availability and scaling.                          |

## Additional ports

The following table shows ports used by other essential tools in the Mimir ecosystem or by specific Mimir components for specialized functions.

| Port      | Function                     | Related components       | Description                                                                                                                                                                              |
| :-------- | :--------------------------- | :----------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 3000      | Grafana UI                   | Grafana                  | The default port for the Grafana visualization platform, commonly deployed alongside Mimir to query and display metrics.                                                                 |
| 9000/9001 | S3-compatible object storage | Minio / Object Storage   | If you're using Minio as your S3-compatible object storage backend for Mimir's blocks, these are its default HTTP API ports. Other S3-compatible storage solutions have their own ports. |
| 9090      | Prometheus UI / API          | Prometheus               | If you're running a Prometheus server to scrape metrics and then remote-write them to Mimir, this is Prometheus's default UI and API port.                                               |
| 9093      | Alertmanager API / UI        | Alertmanager             | If you're using Grafana Mimir's integrated Alertmanager component for alert processing and routing, this is its default port.                                                            |
| 9100      | Node Exporter                | Prometheus Node Exporter | Common port for the Prometheus Node Exporter, which collects host-level metrics. While not directly a Mimir port, it's often used in monitoring a Mimir deployment.                      |
