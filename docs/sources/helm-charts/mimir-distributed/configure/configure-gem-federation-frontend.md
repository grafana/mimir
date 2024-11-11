---
title: "Configure GEM federation-frontend"
menuTitle: "GEM federation-frontend"
description: "Learn how to enable the GEM federation-frontend."
---

# Configure GEM federation-frontend

You can deploy the Grafana Enterprise Metrics (GEM) federation-frontend in a Kubernetes cluster using Helm. The federation-frontend allows you to query data from multiple GEM clusters using a single endpoint. For more information about cluster query federation, refer to the [federation-frontend documentation](https://grafana.com/docs/enterprise-metrics/<GEM_VERSION>/operations/cluster-query-federation).

{{< admonition type="note" >}}
This guide focuses specifically on deploying the federation-frontend component as a standalone deployment without any additional GEM or Mimir components.
{{< /admonition >}}

## Before you begin

1. Set up a GEM cluster: For information about setting up and configuring a GEM deployment, refer to the [Configure Grafana Enterprise Metrics]({{< relref "./configure-grafana-enterprise-metrics" >}}) and [Get started with Grafana Mimir using the Helm chart]({{< relref "../get-started-helm-charts" >}}) articles.
2. Provision an access token with the `metrics:read` scope for each cluster that you want to query. For more information, refer to [Set up a GEM tenant](https://grafana.com/docs/enterprise-metrics/<GEM_VERSION>/set-up-gem-tenant).

## Deploy the GEM federation-frontend

1. Create a Kubernetes `Secret` named `gem-tokens` with the GEM access tokens for each of the remote GEM clusters. The Helm values file uses this `Secret` later. Replace `TOKEN1` and `TOKEN2` with the access tokens for the remote GEM clusters.

   ```yaml
   apiVersion: v1
   kind: Secret
   metadata:
     name: gem-tokens
   data:
     CLUSTER_1_GEM_TOKEN: TOKEN1
     CLUSTER_2_GEM_TOKEN: TOKEN2
   ```

2. Apply the secret to your cluster in the `federation-frontend-demo` namespace with the `kubectl` command:

   ```bash
   kubectl -n federation-frontend-demo apply -f mysecret.yaml
   ```

3. Create a values file named `federation-frontend.yaml` with the following content.

   Replace `http://gem-query-frontend.monitoring.svc.cluster.local:8080/prometheus` and `https://gem.monitoring.acme.local/prometheus` with the URLs of the GEM clusters you want to query. Replace `tenant-1` and `tenant-2` with the tenant IDs of the remote GEM clusters.

   Note that these resource settings are examples that are sufficient for small deployments. Adjust the values based on your specific requirements and load:

   ```yaml
   # Enable enterprise features
   enterprise:
     enabled: true

   # Enable and configure federation-frontend
   federation_frontend:
     enabled: true
     # Since this is a standalone deployment, we configure the chart to not render any of the other GEM components.
     disableOtherComponents: true
     replicas: 2
     resources:
       requests:
         cpu: 100m
         memory: 128Mi
       limits:
         cpu: 1
         memory: 256Mi
     extraEnvFrom:
       - secretRef:
           name: gem-tokens

   # Configure the remote GEM clusters to query.
   mimir:
     structuredConfig:
       auth:
         type: trust
       # The federation-frontend doesn't handle authentication and authorization. Disabling multitenancy means the federation-frontend will not require the X-Scope-OrgID header.
       # With disabled multitenant the federation-frontend will use the authn/z material from the proxy_targets configuration.
       # For a complete list of configuration options, refer to the configuration reference at https://grafana.com/docs/enterprise-metrics/<GEM_VERSION>/config/reference/#federation.
       multitenancy_enabled: false
       federation:
         proxy_targets:
           - name: "cluster-1"
             url: "http://gem-query-frontend.monitoring.svc.cluster.local:8080/prometheus"
             basic_auth:
               username: tenant-1
               password: "${CLUSTER_1_GEM_TOKEN}"

           - name: "cluster-2"
             url: "https://gem.monitoring.acme.local/prometheus"
             basic_auth:
               username: tenant-2
               password: "${CLUSTER_2_GEM_TOKEN}"

   # Disable MinIO
   minio:
     enabled: false

   # The federation-frontend doesn't need the rollout-operator for rollouts, so it can be disabled.
   rollout_operator:
     enabled: false
   ```

4. Deploy the federation-frontend using `helm`:

   ```bash
   helm install federation-frontend grafana/mimir-distributed -f federation-frontend.yaml -n federation-frontend-demo
   ```

   This will deploy only the federation-frontend component. The federation-frontend will be configured to proxy queries to the GEM clusters specified in the `proxy_targets` configuration.

5. Verify that the federation-frontend is running. The simplest way to do this is to issue a label names query against the federation-frontend service.

   This example tries to reach the Kubernetes service from the cluster and request the label names from the past 1 year.

   ```bash
   curl -XPOST 'https://mimir-federation-frontend:8080/prometheus/api/v1/labels' \
     -d "start=$(date -u +%Y-%m-%dT%H:%M:%S.0Z -d '1 year ago' 2>/dev/null || date -u -v -1y +%Y-%m-%dT%H:%M:%S.0Z)" \
     -d "end=$(date -u +%Y-%m-%dT%H:%M:%S.0Z)"
   ```

   You should receive a response with the label names from the remote GEM clusters similar to this:

   ```json
   {
     "status": "success",
     "data": ["__cluster__", "__name__", "hash_extra", "series_id"]
   }
   ```
