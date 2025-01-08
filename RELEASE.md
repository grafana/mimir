# Releases

This document describes the Mimir release process as well as release shepherd responsibilities. Release shepherds are chosen on a voluntary basis.

## Release schedule

A new Grafana Mimir release is cut approximately once every quarter, at the beginning of March, June, September and December.
The following table contains past releases and tentative dates for upcoming releases:

| Version | Date       | Release shepherd   |
| ------- | ---------- | ------------------ |
| 2.0.0   | 2022-03-20 | Marco Pracucci     |
| 2.1.0   | 2022-05-16 | Johanna Ratliff    |
| 2.2.0   | 2022-06-27 | Oleg Zaytsev       |
| 2.3.0   | 2022-08-08 | Tyler Reid         |
| 2.4.0   | 2022-10-10 | Marco Pracucci     |
| 2.5.0   | 2022-11-28 | Mauro Stettler     |
| 2.6.0   | 2023-01-16 | Nick Pillitteri    |
| 2.7.0   | 2023-03-06 | Vernon Miller      |
| 2.8.0   | 2023-04-17 | Jon Kartago Lamida |
| 2.9.0   | 2023-05-29 | Felix Beuke        |
| 2.10.0  | 2023-09-06 | Oleg Zaytsev       |
| 2.11.0  | 2023-12-06 | Justin Lei         |
| 2.12.0  | 2024-03-11 | Yuri Nikolic       |
| 2.13.0  | 2024-06-17 | Dimitar Dimitrov   |
| 2.14.0  | 2024-10-07 | Vladimir Varankin  |
| 2.15.0  | 2024-12-12 | Casie Chen         |
| 2.16.0  | 2025-03-10 | _To be announced_  |

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
  The release shepherd is responsible for going through TODOs in the repository and verifying that nothing that is due this release is forgotten.
- On the planned release date, the release shepherd cuts the first pre-release (using the suffix `-rc.0`) and creates a new branch called `release-<major>.<minor>` starting at the commit tagged for the pre-release.
  New branch `release-<major>-<minor>` should be branched out from latest weekly release `r<xxx>`, where <xxx> is the weekly release number.
  In general, a pre-release is considered a release candidate (that's what `rc` stands for) and should therefore not contain any known bugs that are planned to be fixed in the final release.
- With the pre-release, the release shepherd is responsible for coordinating or running the release candidate in any end user production environment for at least 1 week.
  This is typically done at Grafana Labs.
- If regressions or critical bugs are detected, they need to get fixed before cutting a new pre-release (called `-rc.1`, `-rc.2`, etc.).

See the next section for details on cutting an individual release.

## How to cut an individual release

### Release issue template

The quick and easy way to create a GitHub issue using the following template and follow the instructions from the issue itself.
If something is not clear, you can get back to this document to learn more about the process.

> **Note:** Branches with `release-` prefix in the name are protected, use some different branch name for making your PRs, for example `my/release-notes`.

````markdown
### Publish the release candidate

- [ ] Begin drafting the [release notes](https://github.com/grafana/mimir/blob/main/RELEASE.md#write-release-notes-document)
  - Create the release notes PR targeting the main branch
  - This step shouldn't block from publishing release candidate
  - After the release notes PR is merged (which usually happen after RC is published), cherry-pick them into the release branch
- [ ] Wait for any open PR we want to get merged before cutting the release candidate
  - We shouldn't wait for the open PRs beyond the scheduled release date
- [ ] Update `CHANGELOG.md`
  - [ ] Run `./tools/release/check-changelog.sh LAST-RELEASE-TAG...main` and add missing PRs to CHANGELOG
  - [ ] Ensure CHANGELOG entries are [sorted by type](https://github.com/grafana/mimir/blob/main/docs/internal/contributing/README.md#changelog)
  - [ ] Add a new section for the new release so that `## main / unreleased` is blank and at the top. The new section should say `## x.y.0-rc.0`.
- [ ] Run `./tools/release/notify-changelog-cut.sh CHANGELOG.md`
- [ ] Run `make mixin-screenshots`
  - Before opening the PR, review all updated screenshots and ensure no sensitive data is disclosed
- [ ] Create new release branch
  - [ ] Create the branch
    ```bash
    git checkout r<xxx> # xxx is the latest weekly release
    git checkout -b release-<version>
    git push -u origin release-<version>
    ```
  - [ ] Remove "main / unreleased" section from the CHANGELOG
  - [ ] If a new minor or major version is being released, adjust the settings in the `renovate.json5` configuration on the `main` branch by adding the new version.
        This way we ensure that dependency updates maintain the new version, as well as the latest two minor versions.
        For instance, if versions 3.0 and 2.10 are configured in `renovate.json`, and version 3.1 is being released,
        during the release process `renovate.json5` should keep updated the following branches: `main`, `release-3.1`, `release-3.0` and `release-2.10`.
- [ ] Publish the Mimir release candidate
  - [ ] Update VERSION in the release branch and update CHANGELOG with version and release date.
    - Keep in mind this is a release candidate, so the version string in VERSION and CHANGELOG must end in `-rc.#`, where `#` is the release candidate number, starting at 0.
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
    This prepares a PR into `main` branch. On approval, **use** the `merge-approved-pr-branch-to-main.sh` script, following the [instruction](https://github.com/grafana/mimir/blob/main/RELEASE.md#merging-release-branch-into-main) on how to merge the PR with "Merge commit" (i.e. we DO NOT "Squash and merge" this one).
  - [ ] Publish the Github pre-release draft after getting review from at least one maintainer
  - [ ] Announce the release candidate on social media such as on Mimir community slack using your own Twitter, Mastodon or LinkedIn account
- [ ] Vendor the release commit of Mimir into Grafana Enterprise Metrics (GEM)
  - _This is addressed by Grafana Labs_
- [ ] Publish a `mimir-distributed` Helm chart release candidate. Follow the instructions in [Release process for a release candidate](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/RELEASE.md#release-process-for-a-release-candidate)
- [ ] Promote experimental features to stable and remove deprecated features for the **next** release:
  - [ ] Open a PR into `main` branch for every experimental feature we want to promote to stable
  - [ ] Open a PR into `main` branch with any deprecated feature or configuration option removed in the next release

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
    - NOTE: The release notes should be included at `docs/sources/mimir/release-notes` on the branch _before_ tagging the release.
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
    This prepares a PR into `main` branch. On approval, **use** the `merge-approved-pr-branch-to-main.sh` script, following the [instruction](https://github.com/grafana/mimir/blob/main/RELEASE.md#merging-release-branch-into-main) on how to merge the PR with "Merge commit" (i.e. we DO NOT "Squash and merge" this one).
  - [ ] If during the release process settings in the `renovate.json5` have been modified in such a way that dependency updates maintain more than the latest two minor versions,
        modify it again to ensure that only the latest two minor versions get updated.
        For instance, if versions 3.1, 3.0 and 2.10 are configured in `renovate.json5`, `renovate.json5` should keep updated the following branches:
        `main`, `release-3.1` and `release-3.0`.
  - [ ] Announce the release on socials
  - [ ] Open a PR to add the new version to the backward compatibility integration test (`integration/backward_compatibility.go`)
    - Keep the last 3 minor releases
  - [ ] Open a PR to update the mixin in ["Self-hosted Grafana Mimir" integration](https://grafana.com/docs/grafana-cloud/monitor-infrastructure/integrations/integration-reference/integration-mimir/)
    - _This is addressed by Grafana Labs_
  - [ ] [Publish dashboards to grafana.com](https://github.com/grafana/mimir/blob/main/RELEASE.md#publish-a-stable-release)
    - _This is addressed by Grafana Labs_
  - [ ] After publishing a GEM release publish the `mimir-distributed` Helm chart. Follow the instructions in [Release process for a final release](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/RELEASE.md#release-process-for-a-final-release)
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
1. Run `./tools/release/notify-changelog-cut.sh CHANGELOG.md` to comment on open PRs with a CHANGELOG entry to rebase on `main` and move the CHANGELOG entry to the top under `## main / unreleased`

### Prepare your release

For a new major or minor release, create the corresponding release branch based on the main branch.
For a patch release, work in the branch of the minor release you want to patch.

To prepare a release branch, first create new release branch (release-X.Y) in the Mimir repository from the main commit of your choice,
and then do the following steps on a temporary branch (prepare-release-X.Y) and make a PR to merge said branch into
the new release branch (prepare-release-X.Y -> release-X.Y):

1. Make sure you've a GPG key associated with your GitHub account (`git tag` will be signed with that GPG key)
   - You can add a GPG key to your GitHub account following [this procedure](https://help.github.com/articles/generating-a-gpg-key/)
     - You may run into:
     ```
     error: gpg failed to sign the data
     error: unable to sign the tag
     ```
     - Follow the steps [here](https://docs.github.com/en/authentication/managing-commit-signature-verification/telling-git-about-your-signing-key#telling-git-about-your-gpg-key) to tell Git about your GPG key.
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

### Creating release on GitHub

**How to create the release using the script:**

```bash
git checkout release-<version>

# Ensure you are authenticated and there is not a stale token in the GITHUB_TOKEN environment variable
gh auth login

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

> **Note:** Homebrew's [autobump job](https://github.com/Homebrew/homebrew-core/actions/workflows/autobump.yml)
> automatically updates the [`mimirtool` Homebrew formula](https://github.com/Homebrew/homebrew-core/blob/master/Formula/mimirtool.rb).
> The autobump job runs once every 24 hours, so it might take a day or two for the updated formula to be created and published.

To publish a stable release:

1. Do not change the release branch directly; make a PR to the `release-X.Y` branch with VERSION and any CHANGELOG changes.
   1. Ensure the `VERSION` file has **no** `-rc.X` suffix
   1. Update the Mimir version in the following locations:
      - `operations/mimir/images.libsonnet` (`_images.mimir` and `_images.query_tee` fields)
      - `operations/mimir-rules-action/Dockerfile` (`grafana/mimirtool` image tag)
   1. Update Jsonnet tests: `make build-jsonnet-tests`
   1. Commit updated tests
1. Update dashboard screenshots
   1. Make sure that operations/mimir-mixin-tools/screenshots/.config is configured according to the directions in [operations/mimir-mixin-tools/screenshots/run.sh](https://github.com/grafana/mimir/blob/main/operations/mimir-mixin-tools/screenshots/run.sh)
   1. Make sure that operations/mimir-mixin-tools/serve/.config is configured according to the directions in [operations/mimir-mixin-tools/serve/run.sh](https://github.com/grafana/mimir/blob/main/operations/mimir-mixin-tools/serve/run.sh)
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
1. Check the `README.md` file for any broken links.
1. Open a PR to **add** the new version to the backward compatibility integration test (`integration/backward_compatibility_test.go`)
   - Keep the last 3 minor releases
1. Publish dashboards (done by a Grafana Labs member)
   1. Login to [https://grafana.com](https://grafana.com) with your Grafana Labs account
      1. Make sure your user in the Grafana Labs organization members list has Admin access.
   1. Open [https://grafana.com/orgs/grafana/dashboards](https://grafana.com/orgs/grafana/dashboards)
   1. For each dashboard at `operations/mimir-mixin-compiled/dashboards`:
      1. Open the respective dashboard page
      1. Click "Revisions" tab
      1. Click "Upload new revision" and upload the updated `.json`

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

In case the automated backport failed, follow the steps from grafanabot's comment to do a manual cherry-pick.

### Merging release branch into main

To merge a release branch `release-X.Y` into `main`:

1. Create a PR to merge `release-X.Y` into `main` (_see below_)
2. Once approved, merge the PR with a **Merge** commit through one of the following strategies:
   - Temporarily enable "Allow merge commits" option in "Settings > Options"
   - Locally merge the `merge-release-X.Y-to-main` branch into `main`, and push the changes to `main` back to GitHub. This doesn't break `main` branch protection, since the PR has been approved already, and it also doesn't require removing the protection.
   - Using `tools/release/merge-approved-pr-branch-to-main.sh` script, which will do the merge and push automatically (in a safe way).

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
