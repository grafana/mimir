---
title: "Configure GEM Federation-Frontend"
menuTitle: "GEM Federation-Frontend"
description: "Learn how to enable the GEM Federation-Frontend."
---

# Configure GEM Federation-Frontend

This article explains how to deploy the Grafana Enterprise Metrics (GEM) Federation-Frontend in a Kubernetes cluster using Helm. The Federation-Frontend allows you to query data from multiple GEM clusters using a single endpoint. For more information about cluster query federation, refer to the [Federation-Frontend documentation](https://grafana.com/docs/enterprise-metrics/<GEM_VERSION>/operations/cluster-query-federation).

This guide focuses specifically on deploying the Federation-Frontend component as a standalone deployment without any additional GEM or Mimir components.

## Before you begin

1. Set up a GEM cluster: For information about setting up and configuring a full GEM deployment, refer to the [Configure Grafana Enterprise Metrics]({{< relref "./configure-grafana-enterprise-metrics" >}}) and [Get started with Grafana Mimir using the Helm chart]({{< relref "../get-started-helm-charts" >}}) articles.
2. Provision an access token with `metrics:read` scope for each cluster that you want to query. For more information, refer to [Set up a GEM tenant](https://grafana.com/docs/enterprise-metrics/<GEM_VERSION>/set-up-gem-tenant).

## Deploy the GEM Federation-Frontend

1. Create a Kubernetes `Secret` named `gem-tokens` with the GEM access tokens for each of the remote GEM clusters. This `Secret` will be later used in the Helm values file. Replace `TOKEN1` and `TOKEN2` with the access tokens for the remote GEM clusters.

   ```yaml
   apiVersion: v1
   kind: Secret
   metadata:
     name: gem-tokens
   data:
     CLUSTER_LOCAL_GEM_PASSWORD: TOKEN1
     CLUSTER_REMOTE_GEM_PASSWORD: TOKEN2
   ```

2. Apply the secret to your cluster in the `federation-frontend-demo` namespace with the `kubectl` command:

   ```bash
   kubectl -n federation-frontend-demo apply -f mysecret.yaml
   ```

3. Create a values file named `federation-frontend.yaml` with the following content.

   Replace `http://gem-query-frontend.monitoring.svc.cluster.local:8080/prometheus` and `https://gem.monitoring.acme.local/prometheus` with the URLs of the GEM clusters you want to query. Replace `tenant-1` and `tenant-2` with the tenant IDs of the remote GEM clusters.

   Note that the resource settings shown below are examples that should be sufficient for small deployments. Adjust the values based on your specific requirements and load:

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
           - name: "cluster-local-gem"
             url: "http://gem-query-frontend.monitoring.svc.cluster.local:8080/prometheus"
             basic_auth:
               username: tenant-1
               password: "${CLUSTER_LOCAL_GEM_PASSWORD}"

           - name: "cluster-remote-gem"
             url: "https://gem.monitoring.acme.local/prometheus"
             basic_auth:
               username: tenant-2
               password: "${CLUSTER_REMOTE_GEM_PASSWORD}"

   # Disable MinIO
   minio:
     enabled: false

   # The federation-frontend doesn't need the rollout-operator for rollouts, so it can be disabled.
   rollout_operator:
     enabled: false
   ```

4. Deploy the Federation-Frontend using `helm`:

   ```bash
   helm install federation-frontend grafana/mimir-distributed -f federation-frontend.yaml -n federation-frontend-demo
   ```

   This will deploy only the Federation-Frontend component. The Federation-Frontend will be configured to proxy queries to the GEM clusters specified in the `proxy_targets` configuration.

5. Verify that the Federation-Frontend is running. The simplest way to do this is to issue a label names query against the Federation-Frontend service.

   This example tries to reach the Kubernetes service from within the cluster.
   ```bash
   curl -XPOST 'https://mimir-federation-frontend:8080/prometheus/api/v1/labels' -d 'start=2024-01-01T00:00:00.0Z' -d 'end=2025-01-01T00:00:00.0Z'
   ```

   You should receive a response with the label names from the remote GEM clusters similar to this:
   ```json
   {"status":"success","data":["__cluster__","__name__","hash_extra","series_id"]}
   ```
