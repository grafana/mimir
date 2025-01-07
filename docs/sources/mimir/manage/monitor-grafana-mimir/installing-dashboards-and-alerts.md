---
aliases:
  - ../../operators-guide/monitoring-grafana-mimir/installing-dashboards-and-alerts/
  - ../../operators-guide/visualizing-metrics/installing-dashboards-and-alerts/
  - ../../operators-guide/monitor-grafana-mimir/installing-dashboards-and-alerts/
description: Learn how to install Grafana Mimir dashboards and alerts.
menuTitle: Installing dashboards and alerts
title: Installing Grafana Mimir dashboards and alerts
weight: 30
---

# Installing Grafana Mimir dashboards and alerts

Grafana Mimir is shipped with a comprehensive set of production-ready Grafana [dashboards]({{< relref "./dashboards" >}}) and alerts to monitor the state and health of a Mimir cluster.

## Requirements

- Grafana Mimir dashboards and alerts [require specific labels]({{< relref "./requirements" >}}) to be set by Prometheus or Grafana Alloy when scraping your Mimir cluster metrics
- Some dashboards require recording rules that you should install in your Prometheus

## Install from package

Grafana Mimir provides ready to use Grafana dashboards in the `.json` format and Prometheus alerts in the `.yaml` format, that you can directly import into your Grafana installation and Prometheus config.

The packaged dashboards and alerts have been compiled from the sources using a default configuration and don't allow you to customize the [required metrics label names]({{< relref "./requirements" >}}).
If you need to customize the required metrics label names please choose one of the other installation options.

1. Download [dashboards](https://github.com/grafana/mimir/tree/main/operations/mimir-mixin-compiled/dashboards), [recording rules](https://github.com/grafana/mimir/blob/main/operations/mimir-mixin-compiled/rules.yaml) and [alerts](https://github.com/grafana/mimir/blob/main/operations/mimir-mixin-compiled/alerts.yaml) from Grafana Mimir repository
2. [Import dashboards in Grafana](/docs/grafana/latest/dashboards/export-import/#import-dashboard)
3. Install recording rules and alerts in your Prometheus

## Install from sources

Grafana Mimir dashboards and alerts are built using [Jsonnet](https://jsonnet.org) language and you can compile them from sources.
If you choose this option, you can change the configuration to match your deployment, like customizing the [required label names]({{< relref "./requirements" >}}).

1. Checkout Mimir source code
   ```bash
   git clone https://github.com/grafana/mimir.git
   ```
2. Review the mixin configuration at `operations/mimir-mixin/config.libsonnet`, and apply your changes if necessary.
3. Compile the mixin
   ```bash
   make build-mixin
   ```
4. Import the dashboards saved at `operations/mimir-mixin-compiled/dashboards/` in [Grafana](/docs/grafana/latest/dashboards/export-import/#import-dashboard)
5. Install the recording rules saved at `operations/mimir-mixin-compiled/rules.yaml` in your Prometheus
6. Install the alerts saved at `operations/mimir-mixin-compiled/alerts.yaml` in your Prometheus

## Install dashboards from Jsonnet mixin

In case you're already using Jsonnet to define your infrastructure as a code, you can vendor the Grafana Mimir mixin directly into your infrastructure repository and configure it overriding the `_config` fields.
Given the exact setup really depends on a case-by-case basis, the following instructions are not meant to be prescriptive but just show the main steps required to vendor the mixin.

1. Initialize Jsonnet
   ```bash
   jb init
   ```
2. Install Grafana Mimir mixin
   ```bash
   jb install github.com/grafana/mimir/operations/mimir-mixin@main
   ```
3. Import and configure it
   ```jsonnet
   (import 'github.com/grafana/mimir/operations/mimir-mixin/mixin.libsonnet') + {
     _config+:: {
       // Override the Grafana Mimir mixin config here.
     },
   }
   ```

### Deploy mixin with Terraform

Based on Jsonnet configuration file, you can use [Terraform](https://www.terraform.io/) to deploy the mixin in both Grafana and Grafana Mimir. To do so, you can use [grafana/grafana](https://registry.terraform.io/providers/grafana/grafana/latest), [ovh/mixtool](https://registry.terraform.io/providers/ovh/mixtool/latest), and [ovh/mimirtool](https://registry.terraform.io/providers/ovh/mimirtool/latest) providers.

1. Create a Terraform `main.tf` file:

   ```hcl
   # Specify which providers to use
   terraform {
     required_version = ">= 1.3.5"
     required_providers {
       mixtool = {
         source  = "ovh/mixtool"
         version = "~> 0.1.1"
       }
       mimirtool = {
         source  = "ovh/mimirtool"
         version = "~> 0.1.1"
       }
       grafana = {
         source  = "grafana/grafana"
         version = "~> 1.32.0"
       }
     }
   }

   # Configure providers if needed
   provider "grafana" {
     url  = "http://localhost:9000"
     auth = "admin:admin"
   }

   provider "mimirtool" {
     address   = "http://localhost:9009"
     tenant_id = "anonymous"
   }

   locals {
     mixin_source   = "custom.libsonnet"
     jsonnet_path   = "vendor"
   }

   # Build alerts
   data "mixtool_alerts" "mimir" {
     source       = local.mixin_source
     jsonnet_path = [local.jsonnet_path]
   }

   # Build rules
   data "mixtool_rules" "mimir" {
     source       = local.mixin_source
     jsonnet_path = [local.jsonnet_path]
   }

   # Build dashboards
   data "mixtool_dashboards" "mimir" {
     source       = local.mixin_source
     jsonnet_path = [local.jsonnet_path]
   }

   # Deploy rules
   resource "mimirtool_ruler_namespace" "rules" {
     namespace   = "rules_community"
     config_yaml = data.mixtool_rules.mimir.rules
   }

   # Deploy alerts
   resource "mimirtool_ruler_namespace" "alerts" {
     namespace   = "alerts_community"
     config_yaml = data.mixtool_alerts.mimir.alerts
   }

   # Deploy dashboards
   resource "grafana_dashboard" "mimir" {
     for_each    = data.mixtool_dashboards.mimir.dashboards
     config_json = each.value
   }
   ```

2. Initialize Terraform:
   ```bash
   terraform init
   ```
3. Review the changes that Terraform would apply to your infrastructure:
   ```bash
   terraform plan
   ```
4. Deploy the changes:
   ```bash
   terraform apply
   ```
