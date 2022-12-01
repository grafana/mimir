# Releases

This document describes the Mimir release process as well as release shepherd responsibilities. Release shepherds are chosen on a voluntary basis.

## Release schedule

A new Grafana Mimir release is cut approximately every 6 weeks. The following table contains past releases and tentative dates for upcoming releases:

| Version | Date       | Release shepherd  |
| ------- | ---------- | ----------------- |
| 2.0.0   | 2022-03-20 | Marco Pracucci    |
| 2.1.0   | 2022-05-16 | Johanna Ratliff   |
| 2.2.0   | 2022-06-27 | Oleg Zaytsev      |
| 2.3.0   | 2022-08-08 | Tyler Reid        |
| 2.4.0   | 2022-10-10 | Marco Pracucci    |
| 2.5.0   | 2022-11-28 | Mauro Stettler    |
| 2.6.0   | 2023-01-16 | _To be announced_ |

## Release shepherd responsibilities

The release shepherd is responsible for an entire minor release series, meaning all pre- and patch releases of a minor release.
The process formally starts with the initial pre-release, but some preparations should be made a few days in advance.

- We aim to keep the `main` branch in a working state at all times.
  In principle, it should be possible to cut a release from `main` at any time.
  In practice, things might not work out as nicely.
  A few days before the pre-release is scheduled, the shepherd should check the state of `main` branch.
  Following their best judgement, the shepherd should try to expedite bug fixes that are still in progress but should make it into the release.
  On the other hand, the shepherd may hold back merging last-minute invasive and risky changes that are better suited for the next minor release.
- There may be some actions left to address when cutting this release.
  The release shepherd is responsible for going through TODOs in the repository and verifying that nothing is that is due this release is forgotten.
- On the planned release date, the release shepherd cuts the first pre-release (using the suffix `-rc.0`) and creates a new branch called `release-<major>.<minor>` starting at the commit tagged for the pre-release.
  In general, a pre-release is considered a release candidate (that's what `rc` stands for) and should therefore not contain any known bugs that are planned to be fixed in the final release.
- With the pre-release, the release shepherd is responsible for coordinating or running the release candidate in any end user production environment for at least 1 week.
  This is typically done at Grafana Labs.
- If regressions or critical bugs are detected, they need to get fixed before cutting a new pre-release (called `-rc.1`, `-rc.2`, etc.).

See the next section for details on cutting an individual release.

## How to cut an individual release

### Release issue template

The quick and easy way to create a GitHub issue using the following template and follow the instructions from the issue itself.
If something is not clear, you can get back to this document to learn more about the process.

````markdown
### Publish the release candidate

- [ ] Begin drafting the [release notes](https://github.com/grafana/mimir/blob/main/RELEASE.md#write-release-notes-document)
  - Write release notes to `main`, then cherry pick them into the release branch
  - Don't block on it to publish the release candidate
- [ ] Wait for any open PR we want to get merged before cutting the release candidate
  - [ ] Eventually open a PR for every experimental feature we want to promote to stable
  - [ ] Eventually open a PR to remove any deprecated feature or configuration option that should be removed in this release
- [ ] Update `CHANGELOG.md`
  - [ ] Run `./tools/release/check-changelog.sh LAST-RELEASE-TAG...main` and add missing PRs to CHANGELOG
  - [ ] Ensure CHANGELOG entries are [sorted by type](https://github.com/grafana/mimir/blob/main/docs/internal/contributing/README.md#changelog)
  - [ ] Add a new section for the new release so that `## main / unreleased` is blank and at the top. The new section should say `## x.y.0-rc.0`.
- [ ] Run `./tools/release/notify-changelog-cut.sh`
- [ ] Run `make mixin-screenshots`
  - Before opening the PR, review all updated screenshots and ensure no sensitive data is disclosed
- [ ] Create new release branch
  - [ ] Create the branch
    ```bash
    git checkout main
    git checkout -b release-<version>
    git push -u origin release-<version>
    ```
  - [ ] Remove "main / unreleased" section from the CHANGELOG
- [ ] Publish the Mimir release candidate
  - [ ] Update VERSION in the release branch and update CHANGELOG with version and release date.
  - [ ] [Tag the release](https://github.com/grafana/mimir/blob/main/RELEASE.md#how-to-tag-a-release)
    ```bash
    git checkout release-<version>
    ./tools/release/tag-release.sh
    ```
  - [ ] Wait until the CI pipeline succeeds
  - [ ] [Create a pre-release on GitHub](https://github.com/grafana/mimir/blob/main/RELEASE.md#creating-release-on-github)
    ```bash
    git checkout release-<version>
    ./tools/release/create-draft-release.sh
    ```
  - [ ] [Merge the release branch release-<version> into main](https://github.com/grafana/mimir/blob/main/RELEASE.md#merging-release-branch-into-main)
    ```bash
    ./tools/release/create-pr-to-merge-release-branch-to-main.sh
    ```
  - [ ] Announce the release candidate on socials
- [ ] Publish the [Helm release candidate](https://github.com/grafana/mimir/blob/main/RELEASE.md#release-helm)
  - _Ask people working on Helm to do it_
- [ ] Vendor the release commit of Mimir into Grafana Enterprise Metrics (GEM)
  - _This is addressed by Grafana Labs_

### Publish the stable release

- [ ] Publish the Mimir stable release
  - [ ] [Write release notes](https://github.com/grafana/mimir/blob/main/RELEASE.md#write-release-notes-document)
    - Ensure the any change to release notes in `main` has been cherry picked to the release branch
  - [ ] Update version in release-<version> branch
    - VERSION
    - CHANGELOG
    - `operations/mimir/images.libsonnet` (`_images.mimir` and `_images.query_tee` fields)
    - `operations/mimir-rules-action/Dockerfile` (`grafana/mimirtool` image tag)
  - [ ] [Tag the release](https://github.com/grafana/mimir/blob/main/RELEASE.md#how-to-tag-a-release)
    ```bash
    git checkout release-<version>
    ./tools/release/tag-release.sh
    ```
  - [ ] Wait until the CI pipeline succeeds
  - [ ] [Create a release on GitHub](https://github.com/grafana/mimir/blob/main/RELEASE.md#creating-release-on-github)
    ```bash
    git checkout release-<version>
    ./tools/release/create-draft-release.sh
    ```
  - [ ] [Merge the release branch release-<version> into main](https://github.com/grafana/mimir/blob/main/RELEASE.md#merging-release-branch-into-main)
    ```bash
    ./tools/release/create-pr-to-merge-release-branch-to-main.sh
    ```
  - [ ] Announce the release on socials
  - [ ] Open a PR to add the new version to the backward compatibility integration test (`integration/backward_compatibility_test.go`)
  - [ ] [Publish dashboards to grafana.com](https://github.com/grafana/mimir/blob/main/RELEASE.md#publish-a-stable-release)
- [ ] Publish the [Helm release candidate](https://github.com/grafana/mimir/blob/main/RELEASE.md#release-helm)
  - _Ask people working on Helm to do it_
````

### Branch management and versioning strategy

We use [Semantic Versioning](https://semver.org/).

We maintain a separate branch for each minor release, named `release-<major>.<minor>`, e.g. `release-1.1`, `release-2.0`.

The usual flow is to merge new features and changes into the `main` branch and to merge bug fixes into the latest release branch.
Bug fixes are then merged into `main` from the latest release branch.
The `main` branch should always contain all commits from the latest release branch.
As long as `main` hasn't deviated significantly from the release branch, new commits can also go to `main`, followed by cherry-picking them back into the release branch. See [Cherry-picking changes into release branch](#cherry-picking-changes-into-release-branch).

Maintaining the release branches for older minor releases happens on a best effort basis.

### Show that a release is in progress

This helps ongoing PRs to get their changes in the right place, and to consider whether they need cherry-picking into release branch.

1. Make a PR to update `CHANGELOG.md` on main
   - Add a new section for the new release so that `## main / unreleased` is blank and at the top.
   - The new section should say `## x.y.0-rc.0`.
1. Get this PR reviewed and merged.
1. Run `./tools/release/notify-changelog-cut.sh` to comment on open PRs with a CHANGELOG entry to rebase on `main` and move the CHANGELOG entry to the top under `## main / unreleased`

### Prepare your release

For a new major or minor release, create the corresponding release branch based on the main branch.
For a patch release, work in the branch of the minor release you want to patch.

To prepare a release branch, first create new release branch (release-X.Y) in the Mimir repository from the main commit of your choice,
and then do the following steps on a temporary branch (prepare-release-X.Y) and make a PR to merge said branch into
the new release branch (prepare-release-X.Y -> release-X.Y):

1. Make sure you've a GPG key associated with your GitHub account (`git tag` will be signed with that GPG key)
   - You can add a GPG key to your GitHub account following [this procedure](https://help.github.com/articles/generating-a-gpg-key/)
1. Update the version number in the `VERSION` file to say `X.Y-rc.0`
1. Update `CHANGELOG.md`
   - Ensure changelog entries for the new release are in this order:
     - `[CHANGE]`
     - `[FEATURE]`
     - `[ENHANCEMENT]`
     - `[BUGFIX]`
   - Run `./tools/release/check-changelog.sh LAST-RELEASE-TAG...main` and add any missing PR which includes user-facing changes.
   - `check-changelog.sh` script also reports number of PRs and authors on the top. Note the numbers and include them in the release notes in GitHub.

Once your release preparation PR is approved, merge it to the `release-X.Y` branch, and continue with publishing.

### Write release notes document

Each Grafana Mimir release comes with a release notes that is published on the website. This document is stored in `docs/sources/mimir/release-notes/`,
and contains following sections:

- Features and enhancements
- Upgrade considerations
- Bug fixes

Please write a draft release notes PR, and get it approved by Grafana Mimir's Product Manager (or ask PM to send PR with the document).
Make sure that release notes document for new version is available from the release branch, and not just `main`.
See [PR 1848](https://github.com/grafana/mimir/pull/1848) for an example PR.

### Publish a release candidate

To publish a release candidate:

1. Do not change the release branch directly; make a PR to the release-X.Y branch with VERSION and any CHANGELOG changes.
   1. Ensure the `VERSION` file has the `-rc.X` suffix (`X` starting from `0`).
1. After merging your PR to the release branch, run `./tools/release/tag-release.sh` to tag the new release from the release branch (see [How to tag a release](#how-to-tag-a-release)).
1. Wait until the CI pipeline succeeds (once a tag is created, the release process through GitHub Actions will be triggered for this tag).
1. Merge the release branch `release-x.y` into `main` (see [Merging release branch into main](#merging-release-branch-into-main))
1. Create a pre-release on GitHub. See [Creating release on GitHub](#creating-release-on-github).
1. Create the [Helm release](#release-helm).

### Creating release on GitHub

**How to create the release using the script:**

```bash
git checkout release-<version>

# Then run the following script and follow the instructions:
./tools/release/create-draft-release.sh
```

**How to create the release manually:**

1. Go to https://github.com/grafana/mimir/releases/new to start a new release on GitHub (or click "Draft a new release" at https://github.com/grafana/mimir/releases page.)
1. Select your new tag, use `Mimir <VERSION>` as Release Title. Check that "Previous tag" next to "Generate release notes" button shows previous Mimir release.
   Click "Generate release notes" button. This will pre-fill the changelog for the release.
   You can delete all of it, but keep "New Contributors" section and "Full Changelog" link for later.
1. Release description consists of:
   - "This release contains XX contributions from YY authors. Thank you!" at the beginning.
     You can find the numbers by running `./tools/release/check-changelog.sh LAST-RELEASE-TAG...NEW-RELEASE-TAG`.
     As an example, running the script with `mimir-2.0.0...mimir-2.1.0` argument reports `Found 417 PRs from 47 authors.`.
   - After contributor stats, please include content of the release notes document [created previously](#write-release-notes-document).
   - After release notes, please copy-paste content of CHANGELOG.md file since the previous release.
   - After CHANGELOG, please include "New Contributors" section and "Full Changelog" link at the end.
     Both were created previously by "Generate release notes" button in GitHub UI.
1. Build binaries with `make BUILD_IN_CONTAINER=true dist` and attach them to the release (building in container ensures standardized toolchain).

### Publish a stable release

> **Note:** Technical documentation is automatically published on release tags or release branches with a corresponding
> release tag. The workflow that publishes documentation is defined in [`publish-technical-documentation-release.yml`](.github/workflows/publish-technical-documentation-release.yml).

To publish a stable release:

1. Do not change the release branch directly; make a PR to the `release-X.Y` branch with VERSION and any CHANGELOG changes.
   1. Ensure the `VERSION` file has **no** `-rc.X` suffix
   1. Update the Mimir version in the following locations:
      - `operations/mimir/images.libsonnet` (`_images.mimir` and `_images.query_tee` fields)
      - `operations/mimir-rules-action/Dockerfile` (`grafana/mimirtool` image tag)
1. Update dashboard screenshots
   1. Run `make mixin-screenshots`
   1. Review all updated screenshots and ensure no sensitive data is disclosed
   1. Open a PR
1. After merging your PR to the release branch, run `./tools/release/tag-release.sh` to tag the new release from the release branch (see [How to tag a release](#how-to-tag-a-release)).
1. Wait until the CI pipeline succeeds (once a tag is created, the release process through GitHub Actions will be triggered for this tag)
1. Create a release on GitHub.
   1. See [Creating release on GitHub](#creating-release-on-github) again.
   1. Copy the release notes from pre-release version, with up-to-date CHANGELOG (if there were any changes in release candidates).
   1. Don't forget the binaries, you'll need to build them again for this version.
1. Merge the release branch `release-x.y` into `main` (see [Merging release branch into main](#merging-release-branch-into-main))
1. Open a PR to **add** the new version to the backward compatibility integration test (`integration/backward_compatibility_test.go`)
1. Publish dashboards (done by a Grafana Labs member)
   1. Login to [https://grafana.com](https://grafana.com) with your Grafana Labs account
   1. Open [https://grafana.com/orgs/grafana/dashboards](https://grafana.com/orgs/grafana/dashboards)
   1. For each dashboard at `operations/mimir-mixin-compiled/dashboards`:
      1. Open the respective dashboard page
      1. Click "Revisions" tab
      1. Click "Upload new revision" and upload the updated `.json`
1. Create the [Helm release](#release-helm).

### How to tag a release

**How to tag a release:**

```bash
git checkout release-<version>
./tools/release/tag-release.sh
```

**How it works:**

Every release is tagged with `mimir-<major>.<minor>.<patch>`, e.g. `mimir-2.0.0`.
Note the `mimir-` prefix, which we use to specifically avoid the Go compiler recognizing them as version tags.
We don't want compatibility with Go's module versioning scheme, since it would require us to keep each major version's
code in its own directory beneath the repository root, f.ex. v2/.

### Cherry-picking changes into release branch

To cherry-pick a change (commit) from `main` into release branch we use a GitHub action and labels:

Add a `backport <release-branch>` label to the PR you want to cherry-pick, where `<release-branch>` is the branch name
according to [Branch management and versioning strategy](#branch-management-and-versioning-strategy).
You can add this label before or after the PR is merged. Grafanabot will open a PR targeting the release
branch with the merge commit of the PR you labelled. See [PR#2290 (original PR)](https://github.com/grafana/mimir/pull/2290) and
[PR#2364 (backport PR)](https://github.com/grafana/mimir/pull/2364) for an example pair.

### Merging release branch into main

To merge a release branch `release-X.Y` into `main`:

1. Create a PR to merge `release-X.Y` into `main` (_see below_)
2. Once approved, merge the PR with a **Merge** commit through one of the following strategies:
   - Temporarily enable "Allow merge commits" option in "Settings > Options"
   - Locally merge the `merge-release-X.Y-to-main` branch into `main`, and push the changes to `main` back to GitHub. This doesn't break `main` branch protection, since the PR has been approved already, and it also doesn't require removing the protection.

**How to create the PR using the script:**

```bash
# Run the following script and follow the instructions:
./tools/release/create-pr-to-merge-release-branch-to-main.sh
```

**How to create the PR manually:**

- Create `merge-release-X.Y-to-main` branch **from the upstream `main` branch** locally
- Merge the `release-X.Y` branch into your `merge-release-X.Y-to-main` branch and resolve conflicts
  - Keep the `main`'s `VERSION` file contents
- Make a PR for merging your `merge-release-X.Y-to-main` branch into `main`

## Release helm

### Pre-requisites:

Either:

- Both Mimir and Enterprise Metrics (GEM) have container images released to Docker Hub that line up (same weekly, same RC, same release version).
- Mimir and GEM images do not match, but there are no breaking changes between the two. That is all shared configuration parameters behave the same way. For example GEM has a bugfix in a maintenance version.

### Release process

1. Determine version number

   - Helm [mandates semantic versioning](https://helm.sh/docs/topics/charts/#the-chartyaml-file)
   - Weekly release should have the version: x.y.z-weekly.w (e.g. 3.1.0-weekly.196)
   - Release candidate: x.y.z-rc.w (e.g. 3.1.0.rc.1)
   - Final version: x.y.z (e.g. 3.1.0)

   **Note**: the weekly and RC number must be separated with dot (.) for correct version ordering.

1. To make a release you have to merge a version bump to the Helm chart to a branch that allows Helm release (e.g. main, release-x.y), see current list in [helm-release.yaml](https://github.com/grafana/mimir/blob/main/.github/workflows/helm-release.yaml) github action.

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
