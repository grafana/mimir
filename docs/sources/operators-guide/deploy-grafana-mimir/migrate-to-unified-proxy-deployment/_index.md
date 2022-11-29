---
description: "Migrate to the unified gateway deployment for NGINX and GEM gateway in Helm"
title: "Migrate to the unified gateway deployment for NGINX and GEM gateway in Helm"
menuTitle: "Unified gateway deployment for NGINX and GEM gateway in Helm"
weight: 110
aliases:
  - /docs/mimir/latest/operators-guide/deploying-grafana-mimir/migrate-to-unified-gateway-deployment/
---

# Migrate to the unified gateway deployment for NGINX and GEM gateway in Helm

Version `4.0.0` of the `mimir-distributed` Helm chart adds a new way to deploy the NGINX reverse proxy in front of
Mimir. The NGINX configuration was unified with the GEM (Grafana Enterprise Metrics) gateway configuration. Using a
single section makes it possible to migrate from Mimir to GEM without downtime.

The unification also brings new features to the GEM gateway: OpenShift Route and horizontal autoscaling of the
gateway Pods.

The unified configuration lives in the `gateway` section of the values file. With this we also
deprecate the `nginx` section. It will be removed in `mimir-distributed` version `7.0.0`.

It is possible to migrate from `nginx` to the `gateway` configuration without downtime too. The migration should take
less than 30 minutes. The rest of this article contains a procedure for migrating from the old `nignx` section to
`gateway`.

## Before you begin

Make sure that the version of the `mimir-distributed` Helm chart that you have installed is `4.0.0` or higher.

## Procedure

1. Scale out the `gateway` deployment:

   1. Change your Helm chart values file to enable the `gateway` and increase its replicas:

      1. Set the number of replicas of the gateway Deployment to the number of
         replicas that NGINX is running with.

      For example, if you have deployed 10 NGINX replicas, then
      add the following configuration to your Helm chart values file `custom.yaml`:

      ```yaml
      gateway:
        enabledNonEnterprise: true
        replicas: 10
      ```

   2. Deploy your changes:

      ```bash
      helm upgrade $RELEASE grafana/mimir-distributed -f custom.yaml
      ```

2. Replace the `nginx` deployment with `gateway`:

   1. Disable NGINX by adding or merging the following configuration with your values file:

      ```yaml
      nginx:
        enabled: false
      ```

   2. If you are using the Ingress that the Helm chart provides, then copy the `ingress` section from `nginx` to
      `gateway`, and override the name to the name of the Ingress resource that
      the Helm chart created for NGINX or the GEM gateway.

      Reusing the name allows the `helm` command to retain the existing resource instead of deleting it and
      recreating it under a slightly different name. Retaining the existing resource means that the Ingress
      Controller in your Kubernetes cluster does not need to delete and recreate the backing resources for the
      Ingress, which might take time depending on which Ingress Controller you use.

      In the example that follows, the name of the Ingress
      resource was `mimir-nginx`. Use `kubectl` to get the name of the existing Ingress resource:

      ```bash
      kubectl get ingress
      ```

      ```console
      NAME          CLASS    HOSTS               ADDRESS    PORTS     AGE
      mimir-nginx   <none>   mimir.example.com   10.0.0.1   80, 443   172d
      ```

      The Helm chart values for `gateway` should look similar to this:

      ```yaml
      gateway:
        ingress:
          enabled: true
          nameOverride: mimir-nginx
          hosts:
            - host: mimir.example.com
              paths:
                - path: /
                  pathType: Prefix
          tls:
            - secretName: mimir-gateway-tls
              hosts:
                - mimir.example.com
        enabledNonEnterprise: true
        replicas: 10
      ```

   3. Update the `service` section.

      If you are overriding anything in the `nginx.service` section, then copy the contents of `nginx.service`
      section from `nginx` to `gateway`.

      Next, override the resource name to the name of the Service resource that the chart created for NGINX. Reusing
      the name allows the `helm` command to retain the existing resource instead of deleting it and
      recreating it under a slightly different name. Reusing the name also allows existing clients within the
      Kubernetes cluster to keep using the nginx Service address without disruption.

      In the example that follows, the name of the Service
      resource was `mimir-nginx`. Use `kubectl` to get the name of the existing Service resource:

      ```bash
      kubecl get service
      ```

      ```console
      NAME          TYPE        CLUSTER-IP    EXTERNAL-IP   PORT(S)             AGE
      mimir-nginx   ClusterIP   10.188.8.32   <none>        8080/TCP,9095/TCP   172d
      ```

      After carrying out this step the Helm values for `gateway` should look like the following:

      ```yaml
      gateway:
        service:
          annotations:
            networking.istio.io/exportTo: admin
          nameOverride: mimir-nginx
        ingress:
          enabled: true
          nameOverride: mimir-nginx
          hosts:
            - host: mimir.example.com
              paths:
                - path: /
                  pathType: Prefix
          tls:
            - secretName: mimir-gateway-tls
              hosts:
                - mimir.example.com
        enabledNonEnterprise: true
        replicas: 10
      ```

   4. Update the readiness probe endpoint if you are overriding `nginx.nginxConfig`.

      The readiness probe in the `gateway` setup uses the `/ready` endpoint on the containers. Version `4.0.0` of
      `mimir-distributed` configures the NGINX to serve this endpoint. In versions prior to that that endpoint
      does not exist. If you have copied the contents of `nginx.nginxConfig` into your values file prior
      to version `4.0.0`, then you need to correct the readiness probe.

      After carrying out this step the Helm values for `gateway` should look like the following:

      ```yaml
      gateway:
        readinessProbe:
          httpGet:
            path: /
        service:
          annotations:
            networking.istio.io/exportTo: admin
          nameOverride: mimir-nginx
        ingress:
          enabled: true
          nameOverride: mimir-nginx
          hosts:
            - host: mimir.example.com
              paths:
                - path: /
                  pathType: Prefix
          tls:
            - secretName: mimir-gateway-tls
              hosts:
                - mimir.example.com
        enabledNonEnterprise: true
        replicas: 10
      ```

   5. Move the rest of your values according the following table:

      | Deprecated field                      | New field                               | Notes                                                                  |
      | ------------------------------------- | --------------------------------------- | ---------------------------------------------------------------------- |
      | `nginx.affinity`                      | `gateway.affinity`                      | Previously `affinity` was a string. Now it should be a YAML object.    |
      | `nginx.annotations`                   | `gateway.annotations`                   |                                                                        |
      | `nginx.autoscaling`                   | `gateway.autoscaling`                   |                                                                        |
      | `nginx.basicAuth`                     | `gateway.nginx.basicAuth`               | Nested under `proxy.nginx`.                                            |
      | `nginx.containerSecurityContext`      | `gateway.containerSecurityContext`      |                                                                        |
      | `nginx.deploymentStrategy`            | `gateway.strategy`                      | Renamed from `deploymentStrategy` to `strategy`.                       |
      | `nginx.enabled`                       | `gateway.enabled`                       |                                                                        |
      | `nginx.extraArgs`                     | `gateway.extraArgs`                     |                                                                        |
      | `nginx.extraContainers`               | `gateway.extraContainers`               |                                                                        |
      | `nginx.extraEnvFrom`                  | `gateway.extraEnvFrom`                  |                                                                        |
      | `nginx.extraEnv`                      | `gateway.env`                           | Renamed from `extraEnv` to `env`.                                      |
      | `nginx.extraVolumeMounts`             | `gateway.extraVolumeMounts`             |                                                                        |
      | `nginx.extraVolumes`                  | `gateway.extraVolumes`                  |                                                                        |
      | `nginx.image`                         | `gateway.nginx.image`                   | Nested under `proxy.nginx`.                                            |
      | `nginx.ingress`                       | `gateway.ingress`                       |                                                                        |
      | `nginx.nginxConfig`                   | `gateway.nginx.config`                  | Renamed from `nginxConfig` to `config` and nested under `proxy.nginx`. |
      | `nginx.nodeSelector`                  | `gateway.nodeSelector`                  |                                                                        |
      | `nginx.podAnnotations`                | `gateway.podAnnotations`                |                                                                        |
      | `nginx.podDisruptionBudget`           | `gateway.podDisruptionBudget`           |                                                                        |
      | `nginx.podLabels`                     | `gateway.podLabels`                     |                                                                        |
      | `nginx.podSecurityContext`            | `gateway.securityContext`               | Renamed from `podSecurityContext` to `securityContext`.                |
      | `nginx.priorityClassName`             | `gateway.priorityClassName`             |                                                                        |
      | `nginx.readinessProbe`                | `gateway.readinessProbe`                |                                                                        |
      | `nginx.replicas`                      | `gateway.replicas`                      |                                                                        |
      | `nginx.resources`                     | `gateway.resources`                     |                                                                        |
      | `nginx.route`                         | `gateway.route`                         |                                                                        |
      | `nginx.service`                       | `gateway.service`                       |                                                                        |
      | `nginx.terminationGracePeriodSeconds` | `gateway.terminationGracePeriodSeconds` |                                                                        |
      | `nginx.tolerations`                   | `gateway.tolerations`                   |                                                                        |
      | `nginx.topologySpreadConstraints`     | `gateway.topologySpreadConstraints`     |                                                                        |
      | `nginx.verboseLogging`                | `gateway.nginx.verboseLogging`          | Nested under `proxy.nginx`.                                            |

   6. Upgrade the Helm release with the migrated values file `custom.yaml`. This concludes the migration.

      ```bash
      helm upgrade $RELEASE grafana/mimir-distributed -f custom.yaml
      ```

## Examples

The examples that follow show how your Helm values file changes after migrating from an NGINX setup to a gateway setup.

```yaml
nginx:
  enabled: true
  replicas: 4

  deploymentStrategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 100%
      maxUnavailable: 10%

  affinity: |
    podAntiAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
      - labelSelector:
          matchExpressions:
            - key: noisyNeighbour
              operator: In
              values:
                - 'true'
        topologyKey: 'kubernetes.io/hostname'

  extraEnv:
    - name: SPECIAL_TYPE_KEY
      valueFrom:
        configMapKeyRef:
          name: special-config
          key: SPECIAL_TYPE

  basicAuth:
    enabled: true
    username: user
    password: pass

  image:
    tag: 1.25-alpine

  nginxConfig:
    logFormat: |-
      main '$remote_addr - $remote_user [$time_local]  $status '
      '"$request" $body_bytes_sent "$http_referer" '
      '"$http_user_agent" "$http_x_forwarded_for"';

  podSecurityContext:
    readOnlyRootFilesystem: true

  ingress:
    enabled: true
    hosts:
      - host: mimir.example.com
        paths:
          - path: /
            pathType: Prefix
    tls:
      - secretName: mimir-gateway-tls
        hosts:
          - mimir.example.com
```

The Helm values file after finishing the migration:

```yaml
nginx:
  enabled: false

gateway:
  enabledNonEnterprise: true
  replicas: 4

  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 100%
      maxUnavailable: 10%

  affinity:
    podAntiAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        - labelSelector:
            matchExpressions:
              - key: noisyNeighbour
                operator: In
                values:
                  - "true"
          topologyKey: "kubernetes.io/hostname"

  env:
    - name: SPECIAL_TYPE_KEY
      valueFrom:
        configMapKeyRef:
          name: special-config
          key: SPECIAL_TYPE

  nginx:
    basicAuth:
      enabled: true
      username: user
      password: pass

    image:
      tag: 1.25-alpine

    nginxConfig:
      logFormat: |-
        main '$remote_addr - $remote_user [$time_local]  $status '
        '"$request" $body_bytes_sent "$http_referer" '
        '"$http_user_agent" "$http_x_forwarded_for"';

  securityContext:
    readOnlyRootFilesystem: true

  ingress:
    enabled: true
    nameOverride: mimir-nginx
    hosts:
      - host: mimir.example.com
        paths:
          - path: /
            pathType: Prefix
    tls:
      - secretName: mimir-gateway-tls
        hosts:
          - mimir.example.com
```
