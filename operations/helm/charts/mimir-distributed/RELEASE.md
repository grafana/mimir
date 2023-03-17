# Releases

## Pre-requisites

Either:

- Both Mimir and Enterprise Metrics (GEM) have container images released to Docker Hub that line up (same weekly, same RC, same release version).
- Mimir and GEM images do not match, but there are no breaking changes between the two. That is all shared configuration parameters behave the same way. For example GEM has a bugfix in a maintenance version.

## Release process

1. Determine version number.

   Helm [mandates semantic versioning](https://helm.sh/docs/topics/charts/#the-chartyaml-file).

   - Weekly releases should have the version: `x.y.z-weekly.w` (e.g. `3.1.0-weekly.196`). These are created automatically, but need to be approved and merged manually ([sample PR](https://github.com/grafana/mimir/pull/4309)).
   - Release candidate: `x.y.z-rc.w` (e.g. `3.1.0.rc.1`)
   - Final version: `x.y.z` (e.g. `3.1.0`)

   **Note**: the weekly and RC number must be separated with dot (.) for correct version ordering.

1. For stable versions, create and merge a PR targeting `main` updating the [Helm changelog](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/CHANGELOG.md) and moving `## main / unreleased` items under the section for this release.

   - If there are any deprecated features that should be removed in this release, then verify that they have been removed, and move their deprecation notices into the section for this release.

1. For stable versions, create and push or reuse a branch based on `main` named `mimir-distributed-release-x.y` (eg. `mimir-distributed-release-4.2` for all `4.2.x` releases).

   - This branch will be used as the target branch for the PR you'll create in the next step.
   - The new branch should start from the commit created by the previous step, so that the changelog is in the right state.

1. Update Helm chart and documentation:

   1. Set the image versions in [values.yaml](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/values.yaml) as needed:
      - `image.tag` (Mimir)
      - `enterprise.image.tag` (GEM) - note that unlike the other image tags, GEM image tags start with `v`, eg. `v2.6.0`, not `2.6.0`
      - `smoke_test.image.tag` (Smoke test, usually same as Mimir)
      - `continuous_test.image.tag` (Continuous test, usually same as Mimir)
   1. Set the `version` to the desired chart version number in the Helm [Chart.yaml](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/Chart.yaml), e.g. 4.2.0, determined in the very first step.
   1. Set the `appVersion` to the included version of Mimir in the Helm [Chart.yaml](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/Chart.yaml), e.g. 2.6.0, doesn't have to include non zero patch version.
   1. Update the `home` URL to point to the appropriate documentation version in the Helm [Chart.yaml](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/Chart.yaml). This should be the closest Mimir documentation version, not "latest" or "next" that change content.
   1. For a new stable release, write release notes in [release notes](https://github.com/grafana/mimir/tree/mimir-distributed-release-4.2/docs/sources/helm-charts/mimir-distributed/release-notes). For a patch release to a stable release, update the release notes in question.
   1. Run `make doc`, to update [README.md](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/README.md) from its template
   1. Open a PR.

      For stable versions, ensure your PR targets the `mimir-distributed-release-x.y` branch you created above.

      For all other versions, your PR should target `main`.

   1. Merge the PR after review

   The [release process](https://github.com/grafana/mimir/blob/main/.github/workflows/helm-release.yaml) checks and creates a Git tag formatted as `mimir-distributed-<version>` (e.g. `mimir-distributed-3.1.0-weekly.196`) on the merge commit created when the PR is merged. The release process fails if the tag already exists to prevent releasing the same version with different content. The release is published in the [Grafana helm-charts](https://grafana.github.io/helm-charts/) Helm repository.

   When a `mimir-distributed-x.y.z` stable version tag is pushed, the documentation is [published](https://github.com/grafana/mimir/blob/main/.github/workflows/publish-technical-documentation-release-helm-charts.yml) on the website.

1. For stable versions, once the release tag in Git is created, merge the branch back to `main`, follow the same procedure as for Mimir releases: [Merging release branch into main](https://github.com/grafana/mimir/blob/main/RELEASE.md#merging-release-branch-into-main).
