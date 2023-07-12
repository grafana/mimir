---
title: "Migrate to Kubernetes version 1.25"
menuTitle: "Migrate to Kubernetes 1.25"
description: "Learn how to migrate the Helm chart to Kubernetes version 1.25"
weight: 10
---

# Migrate to Kubernetes version 1.25

This procedure describes how to prepare a `mimir-distributed` Helm chart release for an upgrade to Kubernetes 1.25.

## Background

Kubernetes version 1.25 removes the support for the deprecated [PodSecurityPolicy](https://kubernetes.io/docs/concepts/security/pod-security-policy/) object. You can learn more about this topic by visiting [PodSecurityPolicy Deprecation: Past, Present, and Future](https://kubernetes.io/blog/2021/04/06/podsecuritypolicy-deprecation-past-present-and-future/).

Due to how Helm [works](https://helm.sh/docs/topics/kubernetes_apis/), PodSecurityPolicy objects must already be removed from the release prior to upgrading to Kubernetes version 1.25. If you are using PodSecurityPolicy on Kubernetes 1.24, this is a breaking change. `mimir-distributed` Helm chart versions before 5.0 used PodSecurityPolicy by default in Kubernetes version 1.24.

## Prerequisite

- You have Kubernetes version 1.22, 1.23 or 1.24.
- This procedure is only applicable if `rbac.create` is `true` and `rbac.type` is `psp` in your current Helm values. This was the case by default before Helm chart version 5.0.

## Procedure

1. If `rbac.create` is `false` or `rbac.type` is `scc`, then there is nothing to do, skip the whole procedure.
1. Optionally follow the Kubernetes [Migrate from PodSecurityPolicy to the Built-In PodSecurity Admission Controller](https://kubernetes.io/docs/tasks/configure-pod-container/migrate-from-psp/) guide to replace PodSecurityPolicy.
1. Set the `rbac.create` value to `false`.
1. Upgrade the deployment. The chart will not install PodSecurityPolicy objects anymore.

{{% admonition type="note" %}}
Grafana Mimir does not require any special permissions on the hosts that it
runs on. Because of this, you can deploy it in environments that enforce the
Kubernetes [Restricted security policy](https://kubernetes.io/docs/concepts/security/pod-security-standards/).
{{% /admonition %}}

## Troubleshoot

If you have upgraded to Kubernetes 1.25 and see the following error containing PodSecurityPolicy during a Helm release upgrade:

```
resource mapping not found for name: "mimir" namespace: "" from "":
no matches for kind "PodSecurityPolicy" in version "policy/v1beta1" ensure CRDs are installed first
```
This happens because helm stores the current release in a Secret in the namespace. If the current release contains removed resources the `helm` command fails to determine the current state of the release.

To remove the PodSecurityPolicy from the Helm release history follow this procedure:
1. Optionally follow the Kubernetes [Migrate from PodSecurityPolicy to the Built-In PodSecurity Admission Controller](https://kubernetes.io/docs/tasks/configure-pod-container/migrate-from-psp/) guide to replace PodSecurityPolicy.
1. Set the `rbac.create` value to `false`.
1. Remove PodSecurityPolicy from the Helm release history following the [Updating API Versions of a Release Manifest](https://helm.sh/docs/topics/kubernetes_apis/#updating-api-versions-of-a-release-manifest) Helm documentation in order to proceed with the upgrade.
1. Upgrade the release. The upgrade should succeed now.
