---
description: Tools for Grafana Mimir aid in administration and troubleshooting tasks.
menuTitle: Tools
title: Grafana Mimir tools
weight: 100
---

# Grafana Mimir tools

Grafana Mimir provides several command-line tools designed to assist with administration, troubleshooting, and testing tasks within a Mimir cluster.

Grafana Mimir includes the following tools. For more information, including where to download these tools and how to get started using them, refer to the documentation.

## Listblocks

The listblocks tool is used to list and display details of blocks for a given tenant within Grafana Mimir's object storage. This tool is particularly useful for understanding the state of your data blocks, as it directly downloads `meta.json` files for each block, providing an up-to-date view.

For more information, refer to [Grafana Mimir listblocks](https://grafana.com/docs/mimir/<MIMIR_VERSION>/manage/tools/listblocks/).

## Tenant injector

The tenant injector is a standalone HTTP proxy. Its primary function is to inject the `X-Scope-OrgID` HTTP header with a specified tenant ID into incoming HTTP requests. It then forwards these modified requests to a target URL. This tool is useful during development or troubleshooting scenarios when you need to query data for a specific tenant without manually setting the `X-Scope-OrgID` header for each request.

For more information, refer to [Grafana Mimir tenant injector](https://grafana.com/docs/mimir/<MIMIR_VERSION>/manage/tools/tenant-injector/).

## Query-tee

The query-tee is a standalone tool designed for testing and comparison purposes, specifically when evaluating the query results and performance of two Grafana Mimir clusters.

For more information, refer to [Grafana Mimir query-tee](https://grafana.com/docs/mimir/<MIMIR_VERSION>/manage/tools/query-tee/).

## Mimir-continuous-test

Mimir-continuous test provides continuous testing of Mimir's client-side functionalities, particularly around writing and querying series data.

For more information, refer to [Grafana mimir-continuous-test](https://grafana.com/docs/mimir/<MIMIR_VERSION>/manage/tools/mimir-continuous-test/).

## Mimirtool

Mimirtool is a versatile command-line interface (CLI) tool that enables operators and tenants to perform a variety of common tasks interacting with Grafana Mimir or Grafana Cloud Metrics.

For more information, refer to [Grafana Mimirtool](https://grafana.com/docs/mimir/<MIMIR_VERSION>/manage/tools/mimirtool/).

## Markblocks

Markblocks manages the state of time series data blocks in object storage. This tool helps in maintaining data integrity and managing storage effectively, especially in scenarios involving data corruption or manual cleanup.

For more information, refer to [Grafana Markblocks](https://grafana.com/docs/mimir/<MIMIR_VERSION>/manage/tools/markblocks/).
