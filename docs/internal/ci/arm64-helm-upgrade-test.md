# ARM64 Helm Upgrade Test CI Job

## 1. Overview

A dedicated GitHub Actions job, named `helm-upgrade-test-arm64`, is part of the main `test-build-deploy.yml` workflow. This job is specifically designed to validate the Helm chart upgrade process for Mimir in an ARM64 Kubernetes environment.

Its primary purpose is to ensure that Mimir, when deployed and subsequently upgraded using its Helm charts, functions correctly and maintains stability on the ARM64 architecture. This helps catch ARM64-specific issues related to image compatibility, chart templating for ARM64, and runtime behavior post-upgrade.

## 2. Process

The `helm-upgrade-test-arm64` CI job executes the following key steps:

1.  **Triggering**: It runs automatically after the main `build` job (which produces the Mimir OCI images) completes successfully.
2.  **Environment Setup**:
    *   The job runs on an Ubuntu-based GitHub Actions runner.
    *   It sets up QEMU to enable ARM64 emulation, as the runner itself is typically x86-based.
    *   It installs necessary CLI tools: KinD (Kubernetes in Docker), kubectl, and Helm.
3.  **KinD Cluster Creation**: A single-node KinD cluster is created using an ARM64-specific node image (e.g., `kindest/node:v1.27.3-arm64`). This simulates an ARM64 Kubernetes environment.
4.  **Image Loading**:
    *   The job downloads the "Docker Images" artifact produced by the `build` job. This artifact contains the multi-architecture OCI images for Mimir components.
    *   The ARM64 variants of these OCI images (e.g., `mimir.oci`, `mimirtool.oci`, and images for other components) are loaded into the KinD cluster's container runtime. This is typically done using `skopeo copy` to the local Docker daemon and then `kind load docker-image`.
5.  **Helm Chart Installation (Previous Version)**:
    *   The Grafana Helm chart repository is added.
    *   A previous stable version of the `mimir-distributed` Helm chart (e.g., a 4.x.x version) is installed into the KinD cluster. This sets up a baseline for the upgrade test. MinIO is typically enabled, and global platform settings might be applied for ARM64.
6.  **Helm Chart Upgrade (Current Version)**:
    *   The local `mimir-distributed` Helm chart (from the checked-out repository code, representing the version under test) is packaged.
    *   The Helm release is then upgraded using this local chart. Crucially, the upgrade command overrides image repositories and tags to ensure that the Mimir components use the ARM64 images previously loaded into the KinD cluster.
7.  **Validation**:
    *   **Pod Readiness**: After the upgrade, the job checks that key Mimir pods (e.g., ingester, distributor, querier, alertmanager, ruler) become ready in the KinD cluster.
    *   **Helm Test**: If the `mimir-distributed` chart includes Helm tests (`helm test <release_name>`), these are executed to perform basic functional checks.

## 3. Interpreting Results

*   **Successful Run**: A successful completion of the `helm-upgrade-test-arm64` job indicates that:
    *   The Mimir Helm chart can be successfully upgraded from a previous stable version to the current version on an ARM64 Kubernetes environment.
    *   The ARM64 Mimir images are functional.
    *   Core Mimir components become ready and pass basic `helm test` validations after the upgrade on ARM64.
*   **Job Location**: The job can be found in the "Actions" tab of the Mimir GitHub repository, under the `ci / test-build-deploy.yml` workflow runs. Look for the job named "Helm Upgrade Test ARM64".

## 4. Troubleshooting

If the `helm-upgrade-test-arm64` job fails, consider the following common areas for investigation:

1.  **KinD Cluster Setup Issues**:
    *   Errors related to QEMU setup (check logs from the "Set up QEMU" step).
    *   Problems pulling the ARM64 KinD node image (`kindest/node:vX.Y.Z-arm64`).
    *   Failures during `kind create cluster`. The KinD logs within the CI output can provide more details.

2.  **Image Loading Failures**:
    *   Errors during `skopeo copy` or `kind load docker-image`. This could indicate issues with the OCI artifact or the image names/tags being used. Verify the image names in the artifact against those expected by the loading script.

3.  **Helm Install/Upgrade Errors**:
    *   The CI job logs will contain the output of `helm install` and `helm upgrade` commands. Look for specific error messages from Helm.
    *   Common issues include incorrect Helm values, problems with chart templating for ARM64, or timeouts if components don't start as expected.
    *   Typos or incorrect paths in image overrides (`--set image.repository=...`, `--set image.tag=...`) can cause pods to fail image pulls.

4.  **Pod Readiness Failures**:
    *   If pods do not become ready after the Helm upgrade, the job should ideally dump logs from failing pods.
    *   The `kubectl wait ...` commands for pod readiness might time out.
    *   To debug further (if logs are insufficient), one would typically use:
        *   `kubectl logs <pod-name> -n <namespace>` for the specific failing pod in the KinD cluster.
        *   `kubectl describe pod <pod-name> -n <namespace>` to get events and status conditions for the pod (e.g., ImagePullBackOff, CrashLoopBackOff).
    *   The namespace is usually `mimir`.

5.  **`helm test` Failures**:
    *   If `helm test` fails, the logs for the test pods will provide information about what specific test assertion failed. These logs are usually captured in the CI output.

**Primary Source of Information**:
Always refer to the detailed logs of the failed CI job in GitHub Actions. The logs contain the output of all executed commands and should provide context for the failure.
