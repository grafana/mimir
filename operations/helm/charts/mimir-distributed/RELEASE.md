# Releases

The release processes that follow apply to the Grafana Mimir Helm chart.

## Before you begin

One of the following scenarios must apply:

- Both Grafana Mimir and Grafana Enterprise Metrics (GEM) have container images, whose image versions match. The image versions have the same weekly, RC, and final versions. These versions are released to Docker Hub.
- The Mimir and GEM image versions do not match, but there are no breaking changes between them because all of the shared configuration parameters behave the same way. For example, because GEM contains a bugfix, the version of GEM is `2.5.1` and the version of Mimir is `2.5.0`.

## Release process for a weekly release

Each weekly release is created automatically, but you need to approve and merge it manually. For an example, see [PR 4600](https://github.com/grafana/mimir/pull/4600).

[The Chart.yaml file](https://helm.sh/docs/topics/charts/#the-chartyaml-file) requires semantic versioning.

Weekly releases have the version `x.y.z-weekly.w`, for example `3.1.0-weekly.196`, where `196` represents an incrementing week number.

> **Note**: You must precede the week number (such as `196`) with a dot (`.`).

## Release process for a release candidate or final release

The following steps apply to a release candidate or a final release.

1. Determine the Helm chart version number.

   [The Chart.yaml file](https://helm.sh/docs/topics/charts/#the-chartyaml-file) requires semantic versioning:

   - Release candidates have the version `x.y.z-rc.w`, for example `3.1.0-rc.7`.
   - The final version has the version `x.y.z`, for example `3.1.0`.

   > **Note**: You must precede the release candidate number (such as `7`) with a dot (`.`).

1. If you do not have a release branch, create one:

   a. Create a PR, whose target is `main`, that updates the [Helm chart changelog](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/CHANGELOG.md), and move any `## main / unreleased` items under this release’s version.

   > **Note:** If there are any deprecated features that should be removed in this release, then verify that they have been removed, and move their deprecation notices into the section for this release.

   b. Have the PR reviewed by a maintainer.

   c. Merge the PR upon approval.

   d. Create and push a branch starting from the commit created by the previous PR and name it `mimir-distributed-release-x.y`.

   For example, `mimir-distributed-release-4.5` for any `4.5.x` release.

1. Switch to the release branch.

   For example, `mimir-distributed-release-4.5` for any `4.5.x` release.

1. Set the image versions in [values.yaml](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/values.yaml), as needed:

   - `image.tag` (Mimir)
   - `enterprise.image.tag` (GEM)

     > **Note:** Unlike the Mimir image tags, GEM image tags start with `v`. For example, `v2.6.0` instead of `2.6.0`.

   - `smoke_test.image.tag` (Smoke test; usually the same as Mimir)
   - `continuous_test.image.tag` (Continuous test; usually the same as Mimir)

1. Set the `version` field, in the [Chart.yaml](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/Chart.yaml) file, to the desired version.

   For example, `4.5.0`.

   > **Note:** This is the step that triggers the release process GitHub Action.

1. Set the `appVersion` field, in the [Chart.yaml](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/Chart.yaml) file, to the version of Mimir that the Helm chart deploys.

   For example, `2.6.0`.

1. Create or update the release notes in [release notes](https://github.com/grafana/mimir/tree/mimir-distributed-release-4.2/docs/sources/helm-charts/mimir-distributed/release-notes).

1. From the root directory of the repository, run `make doc` so a template can update the [README.md](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/README.md) file.

1. Open a PR, and make sure that your PR targets the release branch.

1. Have the PR reviewed by a maintainer.

1. Merge the PR upon approval.

1. Verify that the Helm chart is published; run the following commands:

   `helm repo update && helm search repo grafana/mimir-distributed --version <VERSION>`

   You might have to wait a few minutes.

1. In a browser, go to [https://grafana.com/docs/helm-charts/mimir-distributed](https://grafana.com/docs/helm-charts/mimir-distributed) and refresh the page.

1. After the release tag in Git is created, merge the branch back into `main` by following the same procedure as for Mimir releases: [Merging release branch into main](https://github.com/grafana/mimir/blob/main/RELEASE.md#merging-release-branch-into-main).

The [release process](https://github.com/grafana/mimir/blob/main/.github/workflows/helm-release.yaml) checks and creates a Git tag formatted as `mimir-distributed-<version>`, for example `mimir-distributed-4.5.0`, on the merge commit created when the PR is merged. To prevent releasing the same version with different content, the release process fails if the tag already exists. The release is published in the [Grafana helm-charts](https://grafana.github.io/helm-charts/) Helm repository.

When a `mimir-distributed-x.y.z` final version tag is pushed, the Helm chart documentation is [published](https://github.com/grafana/mimir/blob/main/.github/workflows/publish-technical-documentation-release-helm-charts.yml) to [https://grafana.com/docs/helm-charts/mimir-distributed](https://grafana.com/docs/helm-charts/mimir-distributed).
