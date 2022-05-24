---
title: "GrafanaCon 2022 Mimir session"
menuTitle: "GrafanaCon 2022 Mimir session"
description: "Configuration files used during GrafanaCon 20202 Mimir session"
weight: 1
keywords:
  - grafanacon 2022
  - mimir
author:
  - peter
associated_technologies:
  - mimir
---

# Before you start

**Warning:** Following commands will not specify explicit context for `kubectl` commands. Make sure to select correct
context and namespace. For example Docker for Desktop comes with `docker-desktop` context preinstalled. We will
use `default` namespace in this demo.

```
kubectl config use-context docker-desktop
kubectl config set-context docker-desktop --namespace=default
```

# Adding Grafana Helm Charts repository

```
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update
```

# Install Ingress

```
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.1.2/deploy/static/provider/cloud/deploy.yaml
```

# Grafana Agent Operator

Links:

- [Installing Grafana Agent Operator with Helm](https://grafana.com/docs/agent/latest/operator/helm-getting-started/)
- [Grafana Agent Operator Custom Resource Quickstart](https://grafana.com/docs/agent/latest/operator/custom-resource-quickstart/)

```
helm install agent grafana/grafana-agent-operator

kubectl apply -f agent-setup.yaml
```

# Deploy Mimir

`mimir-values.yaml` file uses `mimir-helm` as ingress hostname. Make sure it resolves to IP address of your Ingress.

```
helm install gcon grafana/mimir-distributed -f mimir-values.yaml
```

# Configure Grafana Agent to scrape Mimir metrics and send them to Mimir

```
kubectl apply -f metrics-instance.yaml
```

# Deploy Rules and Alerts

```
mimirtool rules load --address=http://mimir-helm/ --id=anonymous ./rules.yaml

mimirtool rules load --address=http://mimir-helm/ --id=anonymous ./alerts.yaml
```

# Scrape Kubernetes metrics

This step adds more ServiceMonitors, which will be used by Grafana Agent to discover metrics to scrape, and forward to Mimir.
ServiceMonitors in this case are used to scrape metrics from Kubernetes itself.

```
kubectl apply -f kubernetes-service-monitors.yaml
```
