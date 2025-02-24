# Releases

The release processes that follow apply to the Grafana Mimir Helm chart.

## Schedule

The release schedule follows the Grafana Mimir and Grafana Enterprise Metrics (GEM) releases so that a new Helm chart release will include both. This usually happens within 2 weeks of the Mimir release. For the Mimir release schedule consult the Mimir [RELEASE.md](../../../../RELEASE.md).

Security updates of Mimir or GEM will also trigger a Helm chart release. The Helm chart can be released independently of Mimir and GEM, but this is only done if
there's some urgent update needed.

There are weekly releases with the latest Mimir and GEM build, but they are intended for testing purposes only, not production. Weekly releases are marked with a development version and helm will ignore them unless the `--devel` flag is used on the command line.

## Before you begin

One of the following scenarios must apply:

- Both Mimir and GEM have container images, whose image versions match. The image versions have the same weekly, RC, and final versions. These versions are released to Docker Hub.
- The Mimir and GEM image versions do not match, but there are no breaking changes between them because all of the shared configuration parameters behave the same way. For example, because GEM contains a bugfix, the version of GEM is `2.5.1` and the version of Mimir is `2.5.0`.

## Release process for a weekly release

Each weekly release is created automatically, but you need to approve and merge it manually. For an example, see [PR 4600](https://github.com/grafana/mimir/pull/4600).

[The Chart.yaml file](https://helm.sh/docs/topics/charts/#the-chartyaml-file) requires semantic versioning.

Weekly releases have the version `x.y.z-weekly.w`, for example `3.1.0-weekly.196`, where `196` represents an incrementing week number.

> **Note**: You must precede the week number (such as `196`) with a dot (`.`).

## Release process for a release candidate

1. Determine the Helm chart version number.

   [The Chart.yaml file](https://helm.sh/docs/topics/charts/#the-chartyaml-file) requires semantic versioning:

   - Release candidates have the version `x.y.z-rc.w`, for example `3.1.0-rc.7`.

   > **Note**: You must precede the release candidate number (such as `7`) with a dot (`.`).

1. Prepare changelog.

   - Create a PR, whose target is `main`, that updates the [Helm chart changelog](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/CHANGELOG.md), and move any `## main / unreleased` items under this release’s version.

   > **Note:** If there are any deprecated features that should be removed in this release, then verify that they have been removed, and move their deprecation notices into the section for this release.

   - Have the PR reviewed by a maintainer.

   - Merge the PR upon approval.

1. Notify open PRs about the cut change log.

   - From the root directory of the repository run

     ```bash
     ./tools/release/notify-changelog-cut.sh operations/helm/charts/mimir-distributed/CHANGELOG.md
     ```

1. Create a release branch.

   - Create (if the branch is not created yet), switch to and push a branch starting from the commit created by the prepare changelog PR and name it `mimir-distributed-release-x.y`.

     For example, `mimir-distributed-release-4.5` for any `4.5.x` release.

   - Push the branch to origin without any commit added.

     ```bash
     git fetch
     git checkout origin/main
     git checkout -b mimir-distributed-release-<version>
     git push -u origin mimir-distributed-release-<version>
     ```

   - Once the branch is pushed, all changes to `mimir-distributed-release-x.y` branch must be done through PR.

1. Create a branch from release branch if it hasn't been created yet, to update Mimir/GEM image and helm chart version .

   For example `user/update-mimir-distributed-release-x.y`.

1. Update versions and links in the `user/update-mimir-distributed-release-x.y` branch.

   - Set the image versions in [values.yaml](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/values.yaml), as needed:

     - `image.tag` (Mimir)
     - `enterprise.image.tag` (GEM)

       > **Note:** Unlike the Mimir image tags, GEM image tags start with `v`. For example, `v2.6.0` instead of `2.6.0`.

   - Set the `version` field, in the [Chart.yaml](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/Chart.yaml) file, to the desired release candidate version.

     For example, `4.5.0-rc.0`.

     > **Note:** Once this change is merged to `mimir-distributed-x.y` branch, it will trigger the release process GitHub Action. Unless you want to release final release, make sure to append the correct rc version.

   - Set the `appVersion` field, in the [Chart.yaml](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/Chart.yaml) file, to the version of Mimir that the Helm chart deploys.

     For example, `2.6.0`.

   - Add a changelog entry in `mimir-distributed/CHANGELOG.md` about upgading the chart's versions of Mimir and GEM.

   - Create or update the release notes in `docs/sources/helm-charts/mimir-distributed/release-notes` directory.

     The release notes should refer to the correct Mimir and GEM versions and their specific documentation version.

     > **Note:** This step can be done in a separate PR and shouldn't block release candidate from getting published.

   - Update the Mimir and GEM documentation version parameters in [\_index.md](https://github.com/grafana/mimir/blob/main/docs/sources/helm-charts/mimir-distributed/_index.md)

     The two parameters are `MIMIR_VERSION` and `GEM_VERSION`. With the exception of the release notes, the Helm chart documentation should refer to the documentation or Mimir and GEM that is actually included in the Helm chart.

   - From the root directory of the repository, run `make doc` to update [README.md](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/README.md) file.

   - Verify that the links on the [README.md](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/README.md) are correct.

1. Open PR to release branch

   - Create PR that contains all the changes we have so far from `user/update-mimir-distributed-release-x.y` branch and make sure that your PR targets the release branch `mimir-distributed-release-x.y`.

   - Have the PR reviewed by a maintainer.

   - Merge the PR upon approval.

1. Verify that the Helm chart is published

   - Run the following commands:

     `helm repo update && helm search repo grafana/mimir-distributed --devel --version <VERSION>`

     You might have to wait a few minutes.

   - In a browser, go to [https://grafana.com/docs/helm-charts/mimir-distributed](https://grafana.com/docs/helm-charts/mimir-distributed) and refresh the page.

1. After the release tag in Git is created, merge the branch back into `main` by following the same procedure as for Mimir releases: [Merging release branch into main](https://github.com/grafana/mimir/blob/main/RELEASE.md#merging-release-branch-into-main).

1. Backport and additional release candidate.

   - If additional changes need to be added to this release, another release candidate version has to be created.
   - Follow [backport process](https://github.com/grafana/mimir/blob/main/RELEASE.md#cherry-picking-changes-into-release-branch) similar with Mimir release to backport changes from main branch.
   - Go back to step 5. Update versions and links in the user/update-mimir-distributed-release-x.y branch to update the next release candidate.

The [release process](https://github.com/grafana/mimir/blob/main/.github/workflows/helm-release.yaml) checks and creates a Git tag formatted as `mimir-distributed-<version>`, for example `mimir-distributed-4.5.0`, on the merge commit created when the PR is merged. To prevent releasing the same version with different content, the release process fails if the tag already exists. The release is published in the [Grafana helm-charts](https://grafana.github.io/helm-charts/) Helm repository.

## Release process for a final release

1. Determine the Helm chart version number.

   [The Chart.yaml file](https://helm.sh/docs/topics/charts/#the-chartyaml-file) requires semantic versioning:

   - The final version has the version `x.y.z`, for example `3.1.0`.
   - Normally we will proceed the same `x.y.z` version value from the release candidate step.

1. Create a branch from release branch to update Mimir/GEM image and helm chart version.

   For example `user/update-mimir-distributed-release-x.y-final`.

1. Optionally finalize release note and update version in the `user/update-mimir-distributed-release-x.y-final` branch.

   - Update and finalize the release notes in `docs/sources/helm-charts/mimir-distributed/release-notes` directory if there has been some changes after release candidate.

   - Finalize the chart's changelog. Update the title of the release section by setting it to the final release version number.

     For example, `## 4.5.0`.

   - Set the `version` field, in the [Chart.yaml](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/Chart.yaml) file, to the desired final release version.

     For example, `4.5.0`.

     > **Note:** Once this change is merged to `mimir-distributed-x.y` branch, it will trigger the release process GitHub Action.

   - There shouldn't be anymore update needed in documentation because that has been done in the release candidate step above.

     > **Note:** Check that the final versions of Mimir and GEM defined in the chart match those mentioned in the changelog and the release notes.

   - From the root directory of the repository, run `make doc` to update [README.md](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/README.md) file.

1. Open PR to release branch

   - Create PR that contains all the changes we have so far from `user/update-mimir-distributed-release-x.y-final` branch and make sure that your PR targets the release branch `mimir-distributed-release-x.y`.

   - Have the PR reviewed by a maintainer.

   - Merge the PR upon approval.

1. Verify that the Helm chart is published

   - Run the following commands:

     `helm repo update && helm search repo grafana/mimir-distributed --version <VERSION>`

     You might have to wait a few minutes.

   - For stable releases (i.e. excluding release candidates): In a browser, go to the [helm chart docs versions](https://grafana.com/docs/versions/?project=/docs/helm-charts/mimir-distributed/) and verify that the new version is selectable. This might take up to 15 minutes. If this doesn't happen because the `publish-technical-documentation-release-helm-charts` action can't find the release tag, try re-running the workflow on the release branch again after waiting a few minutes.

1. After the release tag in Git is created, merge the branch back into `main` by following the same procedure as for Mimir releases: [Merging release branch into main](https://github.com/grafana/mimir/blob/main/RELEASE.md#merging-release-branch-into-main).

The [release process](https://github.com/grafana/mimir/blob/main/.github/workflows/helm-release.yaml) checks and creates a Git tag formatted as `mimir-distributed-<version>`, for example `mimir-distributed-4.5.0`, on the merge commit created when the PR is merged. To prevent releasing the same version with different content, the release process fails if the tag already exists. The release is published in the [Grafana helm-charts](https://grafana.github.io/helm-charts/) Helm repository.

When a `mimir-distributed-x.y.z` final version tag is pushed, the Helm chart documentation is [published](https://github.com/grafana/mimir/blob/main/.github/workflows/publish-technical-documentation-release-helm-charts.yml) to [https://grafana.com/docs/helm-charts/mimir-distributed](https://grafana.com/docs/helm-charts/mimir-distributed).
