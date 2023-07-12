---
description: "Migrate the Helm chart from version 4.x to 5.0"
title: "Migrate the Helm chart from version 4.x to 5.0"
menuTitle: "Migrate Helm chart 4.x to 5.0"
weight: 110
---

# Migrate the Helm chart from version 4.x to 5.0

The `mimir-distributed` Helm chart version 5.0 and later does not install [PodSecurityPolicy](https://kubernetes.io/docs/concepts/security/pod-security-policy/) objects on Kubernetes version 1.24 by default to prepare for upgrade to Kubernetes version 1.25.

## Prerequisite

- This procedure is only applicable if `rbac.create` is `true` and `rbac.type` is `psp` in your current Helm values. This was the case by default before Helm chart version 5.0.

## Procedure

1. If `rbac.create` is `false` or `rbac.type` is `scc`, then there is nothing to do, skip the whole procedure.
1. Choose between the following options:
   1. If you are on Kubernetes version 1.22 or later and already have [Pod Security Admission](https://kubernetes.io/docs/concepts/security/pod-security-admission/) set up, you don't need to do anything and can upgrade to Helm chart version 5.0.
   1. If you are on Kubernetes version 1.22 or later but don't have [Pod Security Admission](https://kubernetes.io/docs/concepts/security/pod-security-admission/) set up and want to upgrade to Kubernetes version 1.25 after this procedure, then follow the [Migrate to Kubernetes version 1.25]({{< relref "./migrate-to-kubernetes-version-1.25.md" >}}) guide.
   1. If you are on Kubernetes version 1.24 and want to keep using PodSecurityPolicy, then merge the following setting into your custom values file:
      ```yaml
      rbac:
        forcePSPOnKubernetes124: true
      ```

{{% admonition type="warning" %}}
Using `rbac.forcePSPOnKubernetes124` equal `true` value prevents you from upgrading to Kubernetes version 1.25.
{{% /admonition %}}
