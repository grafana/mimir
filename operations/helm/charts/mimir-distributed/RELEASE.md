# Releases

## Pre-requisites

Either:

- Both Mimir and Enterprise Metrics (GEM) have container images released to Docker Hub that line up (same weekly, same RC, same release version).
- Mimir and GEM images do not match, but there are no breaking changes between the two. That is all shared configuration parameters behave the same way. For example GEM has a bugfix in a maintenance version.

## Release process

1. Determine version number

   - Helm [mandates semantic versioning](https://helm.sh/docs/topics/charts/#the-chartyaml-file)
   - Weekly release should have the version: x.y.z-weekly.w (e.g. 3.1.0-weekly.196)
   - Release candidate: x.y.z-rc.w (e.g. 3.1.0.rc.1)
   - Final version: x.y.z (e.g. 3.1.0)

   **Note**: the weekly and RC number must be separated with dot (.) for correct version ordering.

1. To make a release you have to merge a version bump to the Helm chart to a branch that allows Helm release (e.g. main, mimir-distributed-release-x.y), see current list in [helm-release.yaml](https://github.com/grafana/mimir/blob/main/.github/workflows/helm-release.yaml) github action.

   1. For stable versions, the technical documentation and release notes must be finalized in the [documentation](https://github.com/grafana/mimir/tree/main/docs/sources/helm-charts/mimir-distributed) on the release branch.
   1. Set the image versions in [values.yaml](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/values.yaml) as needed. See values:
      - `image.tag` (Mimir)
      - `enterprise.image.tag` (GEM)
      - `smoke_test.image.tag` (Smoke test, usually same as Mimir)
      - `continuous_test.image.tag` (Continuous test, usually same as Mimir)
   1. For stable versions, update the [Helm changelog](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/CHANGELOG.md)
      - If there are any deprecated features that should be removed in this release, then verify that they have been removed, and move their deprecation notices in the section for this release.
   1. Set the `version` to the desired chart version number in the Helm [Chart.yaml](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/Chart.yaml)
   1. Set the `appVersion` to the included version of Mimir in the Helm [Chart.yaml](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/Chart.yaml)
   1. Update the `home` URL to point to the appropriate documentation version in the Helm [Chart.yaml](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/Chart.yaml). This should be the closest Mimir documentation version, not "latest" or "next" that change content.
   1. Run `make doc`, to update [README.md](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/README.md) from its template
   1. Open a PR
   1. Merge the PR after review

   The release process checks and creates a git tag formatted as mimir-distributed-_version_ (e.g. mimir-distributed-3.1.0-weekly.196) on the merge commit. The release process fails if the tag already exists to prevent releasing the same version with different content. The release is published in the [Grafana helm-charts](https://grafana.github.io/helm-charts/) Helm repository.

   When a mimir-distributed-x.y.z stable version tag is pushed, the documentation is [published](https://github.com/grafana/mimir/blob/main/.github/workflows/publish-technical-documentation-release-helm-charts.yml) on the website.
