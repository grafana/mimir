---
aliases:
  - ../../migrate/migrate-from-thanos-to-mimir-with-thanos-sidecar/
description: Learn how to migrate from Thanos to Mimir using Thanos sidecar.
menuTitle: Migrate from Thanos using Thanos sidecar
title: Migrate from Thanos to Mimir using Thanos sidecar
weight: 15
---

# Migrate from Thanos to Mimir using Thanos sidecar

As an operator, you can migrate a deployment of Thanos to Grafana Mimir by using Thanos sidecar to move metrics over with a stepped workflow.

## Overview

An option when migrating is to allow Thanos to query Mimir. This way you retain historical data via your existing Thanos deployment while pointing all of your Prometheus servers (or grafana agents and other metric sources) to Mimir.

The overall setup consists of setting up Thanos Sidecar alongside Mimir and then pointing Thanos Query to the sidecar as if it was just a normal sidecar.

This is not so much of a guide as it is a collection of configurations that have been used with success prior.

**Warning:** This setup is unsupported, experimental and has not been battle tested. Your mileage may vary, ensure you have tested this for your use case and have made appropriate backups (and tested your procedures for recovery)

## Technical details

There are very few obstacles to get this working properly. Thanos Sidecar is designed to work with the Prometheus API which Mimir (almost) implements. There are only 2 endpoints that are not implemented that we need to "spoof" to get Thanos sidecar to believe it is connecting to a Prometheus server: `/api/v1/status/buildinfo` and `/api/v1/status/config`. We spoof these endpoints using NGINX (more details later).

The only other roadblock is the requirement for requests to Mimir to contain the `X-Scope-Org-Id` header to identify the Tenant. We inject this header using another NGINX container sitting in between Thanos Sidecar and Mimir.

In our case, everything was being deployed in Kubernetes. Mimir was deployed using the `mimir-distributed` helm chart. Therefore configurations shown will be Kubernetes manifests that will need to be modified for your environment. If you are not using Kubernetes then you will need to set up the appropriate configuration for NGINX to implement the technical setup described above. You may use the configurations below for inspiration but they will not work out of the box.

## Deploy Thanos sidecar

We deployed the sidecar using Kubernetes using the below manifest:

### Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: thanos-sidecar
  namespace: mimir
  labels:
    app: thanos-sidecar
spec:
  replicas: 1
  selector:
    matchLabels:
      app: thanos-sidecar
  template:
    metadata:
      labels:
        app: thanos-sidecar
    spec:
      volumes:
        - name: config
          configMap:
            name: sidecar-nginx
            defaultMode: 420
      containers:
        - name: nginx
          image: nginxinc/nginx-unprivileged:1.19-alpine
          ports:
            - name: http
              containerPort: 8080
              protocol: TCP
          volumeMounts:
            - name: config
              mountPath: /etc/nginx
          terminationMessagePath: /dev/termination-log
          terminationMessagePolicy: File
          imagePullPolicy: IfNotPresent
        - name: thanos-sidecar
          image: quay.io/thanos/thanos:v0.26.0
          args:
            - sidecar
            - "--prometheus.url=http://localhost:8080/prometheus"
            - "--grpc-address=:10901"
            - "--http-address=:10902"
            - "--log.level=info"
            - "--log.format=logfmt"
          ports:
            - name: http
              containerPort: 10902
              protocol: TCP
            - name: grpc
              containerPort: 10901
              protocol: TCP
      restartPolicy: Always
      terminationGracePeriodSeconds: 30
      dnsPolicy: ClusterFirst
      schedulerName: default-scheduler
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 25%
      maxSurge: 25%
  revisionHistoryLimit: 10
  progressDeadlineSeconds: 600
```

### Service

```yaml
apiVersion: v1
kind: Service
metadata:
  name: thanos-sidecar
  namespace: mimir
  labels:
    app: thanos-sidecar
spec:
  ports:
    - name: 10901-10901
      protocol: TCP
      port: 10901
      targetPort: 10901
  selector:
    app: thanos-sidecar
  type: ClusterIP
```

## NGINX configuration for Thanos sidecar

Notice that there is an NGINX container within the sidecar deployment definition. This NGINX instance sits between the sidecar and Mimir (ie. `sidecar --> nginx --> mimir`) it is responsible for injecting the tenant ID into the requests from the sidecar to Mimir. The configuration that this NGINX instance uses is as follows:

```conf
worker_processes  5;  ## Default: 1
error_log  /dev/stderr;
pid        /tmp/nginx.pid;
worker_rlimit_nofile 8192;

events {
  worker_connections  4096;  ## Default: 1024
}

http {
  client_body_temp_path /tmp/client_temp;
  proxy_temp_path       /tmp/proxy_temp_path;
  fastcgi_temp_path     /tmp/fastcgi_temp;
  uwsgi_temp_path       /tmp/uwsgi_temp;
  scgi_temp_path        /tmp/scgi_temp;

  default_type application/octet-stream;
  log_format   main '$remote_addr - $remote_user [$time_local]  $status '
        '"$request" $body_bytes_sent "$http_referer" '
        '"$http_user_agent" "$http_x_forwarded_for"';
  access_log   /dev/stderr  main;

  sendfile     on;
  tcp_nopush   on;
  resolver kube-dns.kube-system.svc.cluster.local;

  server {
    listen 8080;

    # Distributor endpoints
    location / {
      proxy_set_header X-Scope-OrgID 1;
      proxy_pass      http://mimir-distributed-nginx.mimir.svc.cluster.local:80$request_uri;
    }
  }
}

```

## NGINX configuration for Mimir

Now we modified the Mimir Distributed NGINX configuration to allow the sidecar to fetch the "external labels" and the "version" of our "Prometheus" server. You should be careful with the external labels to make sure that you don't override any existing labels (in our case we used "source='mimir'"). You should also consider existing dashboards and make sure that they will continue to work as expected. Make sure to test your setup first.

In particular, these location blocks were added (full config further down):

```conf
location /prometheus/api/v1/status/config {
        add_header Content-Type application/json;
        return 200 "{\"status\":\"success\",\"data\":{\"yaml\": \"global:\\n  external_labels:\\n    source: mimir\"}}";
    }

location /prometheus/api/v1/status/buildinfo {
        add_header Content-Type application/json;
        return 200 "{\"status\":\"success\",\"data\":{\"version\":\"2.35.0\",\"revision\":\"6656cd29fe6ac92bab91ecec0fe162ef0f187654\",\"branch\":\"HEAD\",\"buildUser\":\"root@cf6852b14d68\",\"buildDate\":\"20220421-09:53:42\",\"goVersion\":\"go1.18.1\"}}";
    }
```

You need to modify the first one to set your external labels. The second one just allows Thanos to detect the version of Prometheus running.

To modify the external labels alter this string: `"{\"status\":\"success\",\"data\":{\"yaml\": \"global:\\n external_labels:\\n source: mimir\"}}"`. You will notice it is escaped JSON. Simply add elements to the `data.external_labels` field to add more external labels as required or alter the existing key to modify them.

### Full Mimir distributed NGINX configuration

```conf
worker_processes  5;  ## Default: 1
error_log  /dev/stderr;
pid        /tmp/nginx.pid;
worker_rlimit_nofile 8192;

events {
  worker_connections  4096;  ## Default: 1024
}

http {
  client_body_temp_path /tmp/client_temp;
  proxy_temp_path       /tmp/proxy_temp_path;
  fastcgi_temp_path     /tmp/fastcgi_temp;
  uwsgi_temp_path       /tmp/uwsgi_temp;
  scgi_temp_path        /tmp/scgi_temp;

  default_type application/octet-stream;
  log_format   main '$remote_addr - $remote_user [$time_local]  $status '
        '"$request" $body_bytes_sent "$http_referer" '
        '"$http_user_agent" "$http_x_forwarded_for"';
  access_log   /dev/stderr  main;

  sendfile     on;
  tcp_nopush   on;
  resolver kube-dns.kube-system.svc.cluster.local;

  server {
    listen 8080;

    location = / {
      return 200 'OK';
      auth_basic off;
    }

    # Distributor endpoints
    location /distributor {
      proxy_pass      http://mimir-distributed-distributor.mimir.svc.cluster.local:8080$request_uri;
    }
    location = /api/v1/push {
      proxy_pass      http://mimir-distributed-distributor.mimir.svc.cluster.local:8080$request_uri;
    }

    # Alertmanager endpoints
    location /alertmanager {
      proxy_pass      http://mimir-distributed-alertmanager.mimir.svc.cluster.local:8080$request_uri;
    }
    location = /multitenant_alertmanager/status {
      proxy_pass      http://mimir-distributed-alertmanager.mimir.svc.cluster.local:8080$request_uri;
    }
    location = /api/v1/alerts {
      proxy_pass      http://mimir-distributed-alertmanager.mimir.svc.cluster.local:8080$request_uri;
    }

    # Ruler endpoints
    location /prometheus/config/v1/rules {
      proxy_pass      http://mimir-distributed-ruler.mimir.svc.cluster.local:8080$request_uri;
    }
    location /prometheus/api/v1/rules {
      proxy_pass      http://mimir-distributed-ruler.mimir.svc.cluster.local:8080$request_uri;
    }

    location /api/v1/rules {
      proxy_pass      http://mimir-distributed-ruler.mimir.svc.cluster.local:8080$request_uri;
    }
    location /prometheus/api/v1/alerts {
      proxy_pass      http://mimir-distributed-ruler.mimir.svc.cluster.local:8080$request_uri;
    }
    location /prometheus/rules {
      proxy_pass      http://mimir-distributed-ruler.mimir.svc.cluster.local:8080$request_uri;
    }
    location = /ruler/ring {
      proxy_pass      http://mimir-distributed-ruler.mimir.svc.cluster.local:8080$request_uri;
    }

    location /prometheus/api/v1/status/config {
      add_header Content-Type application/json;
      return 200 "{\"status\":\"success\",\"data\":{\"yaml\": \"global:\\n  external_labels:\\n    source: mimir\"}}";
    }

    location /prometheus/api/v1/status/buildinfo {
    add_header Content-Type application/json;
    return 200 "{\"status\":\"success\",\"data\":{\"version\":\"2.35.0\",\"revision\":\"6656cd29fe6ac92bab91ecec0fe162ef0f187654\",\"branch\":\"HEAD\",\"buildUser\":\"root@cf6852b14d68\",\"buildDate\":\"20220421-09:53:42\",\"goVersion\":\"go1.18.1\"}}";
    }



    # Rest of /prometheus goes to the query frontend
    location /prometheus {
      proxy_pass      http://mimir-distributed-query-frontend.mimir.svc.cluster.local:8080$request_uri;
    }

    # Buildinfo endpoint can go to any component
    location = /api/v1/status/buildinfo {
      proxy_pass      http://mimir-distributed-query-frontend.mimir.svc.cluster.local:8080$request_uri;
    }
  }
}
```
