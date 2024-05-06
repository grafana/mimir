---
description:
  Learn how to configure Grafana Mimir's cluster label to prevent the Memberlist gossip ring to join
  different memberlist cluster.
title: "Configure a unique Grafana Mimir's Memberlist cluster label in helm installation"
weight: 110
---

# Configure a unique Grafana Mimir's Memberlist cluster label in helm installation

Grafana Mimir uses [memberlist] to share works and deciding which component replica to send the workload such as when ingesting series. 
Memberlist encodes the replica information in data structure called [hash ring], this is a consistent hashing scheme in which different ingesters instance tokens are placed around the ring. 
The token position in the ring determines which ingester should handle a request. 
The information of this hash ring is delivered to different components by using gossip protocol.

By default, hash ring is global. If we also have Grafana Tempo and Grafana Loki in the same Kubernetes cluster, their components can unintentionally talk and sending data with each other, because Loki and Tempo also uses memberlist. 
This possibility to happen increases in cluster where the pods may reuse each other's IPs after churning.

In this document we will describe the step on how to migrate a Mimir installation to prevent two separate memberlist gossip hash ring to join into one.

There are three steps of the migration which we will describe in details in the migration section. 
But in brief summary the steps are:

1. Disable memberlist cluster label verification
1. Set cluster label to all Mimir components
1. Enable memberlist cluster label verification again

The migration should take around 30 minutes. 
The risk of not doing this migration if you have Mimir and Tempo or Loki in the same cluster is the possibility of those different backend to talk with each other.

## How The Migration Solve The Issue

Non-Mimir component can merge with Mimir memberlist because by default, memberlist is not namespaced and applies globally. 
As an example, consider a Loki pod is terminated and the IP address reused by a new Mimir pod in the same cluster. 
This might cause other Loki components try to talk and sending data to this new Mimir pod.

Cluster label will prevent such situation by only allowing communication for components that has same cluster label. 
Once that is enabled, before memberlist try to communicate with other components, it will verify whether that new components are having same cluster label and only allowing memberlist to communicate if the parties are having the same cluster label.

## Migration

### 1. Disable memberlist cluster label verification

Cluster label verification flag is enabled by default. 
However, cluster label default value is an empty string. 
If different systems that using memberlist and not setting the cluster label, they can talk with each other. 
To disable cluster label verification flag, set the following structured config.

```yaml
mimir:
  structuredConfig:
    memberlist:
      cluster_label_verification_disabled: true
```

Rollout the installation to apply the configuration changes by running `helm upgrade <my-mimir-release> mimir-distributed -f values.yaml`.

### 2. Set cluster label to all Mimir components

Set cluster label to all Mimir components by setting the following configuration.
The configuration will set `cluster_label` to the Helm release name and the namespace where the helm release is installed.
Once the configuration is applied, all Mimir components will only communicate via memberlist if the other component is having the same cluster label.

```yaml
mimir:
  structuredConfig:
    memberlist:
      cluster_label_verification_disabled: true
      cluster_label: '{{.Release.Name}}-{{.Release.Namespace}}'
```

Apply the configuration changes by running `helm upgrade <my-mimir-release> mimir-distributed -f values.yaml`.

### 3. Enable memberlist cluster label verification

Remove `mimir.structuredCOnfig.memebrlist.cluster_label_verification_disabled` from the values.yaml file to re-enable Memberlist cluster label verification.

```yaml
mimir:
  structuredConfig:
    memberlist:
      cluster_label: '{{.Release.Name}}-{{.Release.Namespace}}'
```

Apply the configuration changes by running `helm upgrade <my-mimir-release> mimir-distributed -f values.yaml`.

## Verifying The Migration

Once the rollout is completed, verify the change by looking at the `/memberlist` in some pods. 
Run the following port-forward command on several different Grafana Mimir components.

```bash
   kubectl port-forward pod/<mimir-pod-1> --kube-context=<my-k8s-context> --namespace=<my-mimir-namespace> 8080:80
   kubectl port-forward pod/<mimir-pod-2> --kube-context=<my-k8s-context> --namespace=<my-mimir-namespace> 8081:80
```

Replace mimir-pod with several actual pods from different Mimir components. 
Ensure the host port 8080 and 8081 are available, otherwise use different available ports. 
Make sure that the container port which is set by default to port 80 is also correct.

Open the port-forwarded URL in browser to see the memberlist status http://localhost:8080/memberlist, http://localhost:8081/memberlist and also 
few others Grafana mimir components. The member list page must show same view of all of their members.

{{% docs/reference %}}
[memberlist]: "/ -> /docs/mimir/<MIMIR_DOCS_VERSION>/references/architecture/memberlist-and-the-gossip-protocol"
[hash ring]: "/ -> /docs/mimir/<MIMIR_DOCS_VERSION>/references/architecture/hash-ring"
{{% /docs/reference %}}
