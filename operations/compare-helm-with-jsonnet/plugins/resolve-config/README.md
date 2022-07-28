# resolve-config

This program implements the [exec KRM function](https://kubectl.docs.kubernetes.io/guides/extending_kustomize/exec_krm_functions/) interface.

At a high level, these functions take a list of kubernetes resources as input on stdin and output a list of kubernetes resources on stdout.
`resolve-config` attempts to derive the final Grafana Mimir configuration parameters based on the various Deployments, StatefulSets, ConfigMaps, and Secrets given as input.

The output contains all original objects unchanged plus an object of kind `grafana.com/v1alpha1.MimirConfig` for each container using the `grafana/mimir` image.
Any parameters with values matching the defaults will be annotated to include the text `(default)`.
The resulting output is not intended to be a valid mimir configuration, but instead is meant to be useful for comparison via `diff` only.

For example, input containing the following two documents:

```yaml
---
apiVersion: v1
kind: Deployment
metadata:
  name: querier
  namespace: default
spec:
  template:
    spec:
      containers:
        - name: mimir
          image: grafana/mimir:latest
          args:
            - -target=querier
            - -config.file=/etc/mimir/mimir.yaml
          volumeMounts:
            - name: mimir-config
              mountPath: /etc/mimir
      volumes:
        - name: mimir-config
          configMap:
            name: config
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: config
  namespace: default
data:
  mimir.yaml: |
    memberlist:
      bind_port: 1234
```

Will result in output similar to:

```yaml
---
apiVersion: v1
kind: Deployment
metadata:
  name: querier
  namespace: default
spec:
  template:
    spec:
      containers:
        - name: mimir
          image: grafana/mimir:latest
          args:
            - -target=querier
            - -config.file=/etc/mimir/mimir.yaml
          volumeMounts:
            - name: mimir-config
              mountPath: /etc/mimir
      volumes:
        - name: mimir-config
          configMap:
            name: config
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: config
  namespace: default
data:
  mimir.yaml: |
    memberlist:
      bind_port: 1234
---
apiVersion: grafana.com/v1alpha1
kind: MimirConfig
metadata:
  name: querier
  namespace: default
config:
  target: querier
  memberlist:
    abort_if_cluster_join_fails: false (default)
    advertise_addr: " (default)"
    advertise_port: 7946 (default)
    bind_port: 1234
  # remaining configuration fields omitted for brevity
```
