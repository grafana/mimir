---
description:
  Learn how to configure Helm installed Grafana Mimir's cluster label to prevent the Mimir components to join
  different Memberlist cluster.
menuTitle: "Configure a unique Memberlist cluster label"
title: "Configure a unique Grafana Mimir's Memberlist cluster label in the mimir-distributed Helm chart installation"
weight: 110
---

# Configure a unique Grafana Mimir's Memberlist cluster label in the mimir-distributed Helm chart installation

This document shows the steps to configure cluster label verification in a Grafana Mimir installed by Helm.
Multiple [Memberlist](https://grafana.com/docs/mimir/<MIMIR_VERSION>/references/architecture/memberlist-and-the-gossip-protocol/) [gossip ring](https://grafana.com/docs/mimir/<MIMIR_VERSION>/references/architecture/hash-ring/) clusters are at risk of merging into one without enabling cluster label verification.
For example, if a Mimir, Tempo or Loki are running in the same Kubernetes cluster, they might communicate with each other without this configuration update.
Once cluster label verification is enabled, before Mimir components communicate with other components, they will verify whether the other components have the same cluster label.
The process to update the configuration will take three rollouts of the whole cluster.

## Before you begin

- You have a Grafana Mimir installed by mimir-distributed helm chart with its Memberlist cluster label still set to default value.
- You have `kubectl` and `helm` command line configured to connect to the Kubernetes cluster where your Grafana Mimir is running.

## Configuration update steps

There are three steps of the configuration update:

1. Disable Memberlist cluster label verification
1. Set cluster label on all Mimir components
1. Enable Memberlist cluster label verification again

### 1. Disable Memberlist cluster label verification

Cluster label verification flag is enabled by default with cluster label set to an empty string.
Using the default value of cluster label can make different systems that use Memberlist communicate with each other if they also have not updated the default cluster label.
Setting a new cluster label directly to a non-empty string value without first disabling cluster label verification will cause Memberlist to form partition in the Grafana Mimir cluster.
The partition makes some Mimir components have different cluster label values which can prevent the component from communicating.
To disable cluster label verification flag, set the following structured config in mimir-distributed values.yaml configuration.

```yaml
mimir:
  structuredConfig:
    memberlist:
      cluster_label_verification_disabled: true
```

Rollout the installation to apply the configuration changes by running `helm upgrade <my-mimir-release> mimir-distributed -f values.yaml`.
Replace `<my-mimir-release>` with the actual Mimir release name. Wait until all Pods are ready before going to the next step.

### 2. Set cluster label on all Mimir components

Set cluster label on all Mimir components by setting the following configuration.
The configuration will set `cluster_label` to the Helm release name and the namespace where the helm release is installed.
Updating a new cluster label after disabling cluster label verification will prevent Memberlist from forming a partition.

```yaml
mimir:
  structuredConfig:
    memberlist:
      cluster_label_verification_disabled: true
      cluster_label: "{{.Release.Name}}-{{.Release.Namespace}}"
```

Apply the configuration changes again by running `helm upgrade <my-mimir-release> mimir-distributed -f values.yaml`.
Replace `<my-mimir-release>` with the actual Mimir release name. Wait until all Pods are ready before going to the next step.

### 3. Enable Memberlist cluster label verification

Remove `mimir.structuredConfig.memberlist.cluster_label_verification_disabled` from the values.yaml file to re-enable Memberlist cluster label verification.

```yaml
mimir:
  structuredConfig:
    memberlist:
      cluster_label: "{{.Release.Name}}-{{.Release.Namespace}}"
```

Apply the configuration changes by running `helm upgrade <my-mimir-release> mimir-distributed -f values.yaml`.
Replace `<my-mimir-release>` with the actual Mimir release name. Wait until all Pods are ready before verifying that the configuration is applied correctly.

## Verifying the configuration changes

Once the rollout is completed, verify the change by looking at the `/memberlist` endpoint in some of Grafana Mimir pods.
Run the following port-forward command on several different Grafana Mimir components.

```bash
   kubectl port-forward pod/<mimir-pod-1> --kube-context=<my-k8s-context> --namespace=<my-mimir-namespace> 8080:8080
   kubectl port-forward pod/<mimir-pod-2> --kube-context=<my-k8s-context> --namespace=<my-mimir-namespace> 8081:8080
```

Replace `<mimir-pod-1>` and `<mimir-pod-2>` with several actual pods from different Mimir components.
Ensure the host port 8080 and 8081 are available, otherwise use different available ports.

Open the port-forwarded URL in browser to see the Memberlist status http://localhost:8080/memberlist, http://localhost:8081/memberlist and also
few others Grafana Mimir components. The Memberlist page from different pods must show same view of all of their members.
