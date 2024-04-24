---
description: "Migrating Mimir Helm Installation to Use Memberlist Cluster Label"
title: "Migrating Mimir Helm Installation to Use Memberlist Cluster Label"
menuTitle: "Migrating Mimir Helm Installation to Use Memberlist Cluster Label"
weight: 110
---

# Migrating Mimir Helm Installation to Use Memberlist Cluster Label

## Introduction

Grafana Mimir uses memberlist to decide where the series should go when ingesting series. Memberlist encodes the replica information in data structure called hash ring, this is a consistent hashing in which different ingesters instance is placed around the ring and each ingester will have tokens. The combination of token and ingester position determines which ingester should handle a request. The information of this hash ring is delivered to different components by using gossip protocol.

By default hash ring is global. If we also have Grafana Tempo and Grafana Loki in the same Kubernetes cluster, their components can unintentionally talk and sending data with each other (which is not compatible), because Loki and Tempo also uses memberlist. (TODO: More generally in any cluster where the pods may reuse each other's IPs after churning).

In this document we will describe the step on how to migrate a Mimir installation to prevent non Mimir installation to talk to its memberlist cluster.

There are three steps of the migration which we will describe in details in the migration section. But in brief summary the steps are:

1. Disable memberlist cluster label verification
1. Set cluster label to all mimir components
1. Enable memberlist cluster label verification again

The migration should take around 30 minutes. The risk of not doing this migration if you have Mimir and Tempo or Loki in the same cluster is the possibility of those different backend to talk with each other.

## How The Migration Solve The Issue

Non-Mimir component can merge with Mimir memberlist because by default memberlist applies globally. As an example, consider a loki pod is terminated and the IP address reused by a new mimir pod in the same cluster. This might cause other Loki components try to talk and sending data to this new Mimir pod.

Cluster label will prevent such situation by only allowing communication for components that has same cluster label. Once that is enabled, before memberlist try to communicate with other components, it will verify whether that new components are having same cluster label and only allowing memberlist to communicate if the parties are having the same cluster label.

## Migration

### 1. Disable memberlist cluster label verification

By default memberlist will verify the cluster label. But by default too, all cluster labels are an empty string which means, if different systems that using memberlist and not setting the cluster label, they can talk with each other. The very first step is for us to disable cluster label verification flag. In helm we do this by setting the following structured config. In mimir-distributed helm chart version x.x.x this value will be the default hence you don't have to do this step at all.

```
mimir
  structuredConfig:
    memberlist:
      cluster_label_verification_disabled: true
```

Make sure to rollout the installation to apply the configuration changes by running `helm upgrade mimir-distributed -f values.yaml`.

### 2. Set cluster label to all mimir components

Set cluster label to all mimir components by setting the following configuration. Later after applying this configuration changes, and enabling cluster label verification again, all Mimir components will only communicate via memberlist if the other component is having the same cluster label.

```
mimir
  structuredConfig:
    memberlist:
      cluster_label_verification_disabled: true
      cluster_label: "$helm-release-name"
```

Apply the configuration changes by running `helm upgrade mimir-distributed -f values.yaml`.

### 3. Enable memberlist cluster label verification

Set `memberlist.cluster_label_verification_disabled` to false to enable again memberlist cluster label verification.

```
mimir
  structuredConfig:
    memberlist:
      cluster_label_verification_disabled: false
      cluster_label: "$helm-release-name"
```

Apply the configuration changes by running `helm upgrade mimir-distributed -f values.yaml`.

## Verifying The Migration

Once the rollout has been done run the following helm command to verify if we have successfully migrate the Mimir memberlist to check the cluster label.

```
helm --kube-context=k3d-mimir --namespace=x-mimir get values [your-mimir-release-name]
```

You should see the following values as what we have set above.

```
mimir
  structuredConfig:
    memberlist:
      cluster_label_verification_disabled: false
      cluster_label: "$helm-release-name"
```
