# Releases

This document describes the Mimir release process as well as release shepherd responsibilities. Release shepherds are chosen on a voluntary basis.

## Release schedule

A new Grafana Mimir release is cut approximately every 6 weeks. The following table contains past releases and tentative dates for upcoming releases:

| Version | Date       | Release shepherd  |
| ------- | ---------- | ----------------- |
| 2.0.0   | 2022-03-20 | Marco Pracucci    |
| 2.1.0   | 2022-05-16 | Johanna Ratliff   |
| 2.2.0   | 2022-06-27 | _To be announced_ |
| 2.3.0   | 2022-08-08 | _To be announced_ |

## Release shepherd responsibilities

The release shepherd is responsible for an entire minor release series, meaning all pre- and patch releases of a minor release. The process formally starts with the initial pre-release, but some preparations should be made a few days in advance.

- We aim to keep the main branch in a working state at all times. In principle, it should be possible to cut a release from main at any time. In practice, things might not work out as nicely. A few days before the pre-release is scheduled, the shepherd should check the state of main. Following their best judgement, the shepherd should try to expedite bug fixes that are still in progress but should make it into the release. On the other hand, the shepherd may hold back merging last-minute invasive and risky changes that are better suited for the next minor release.
- There may be some actions left to address when cutting this release. The release shepherd is responsible for going through TODOs in the repository and verifying that nothing is that is due this release is forgotten.
- On the planned release date, the release shepherd cuts the first pre-release (using the suffix `-rc.0`) and creates a new branch called `release-<major>.<minor>` starting at the commit tagged for the pre-release. In general, a pre-release is considered a release candidate (that's what `rc` stands for) and should therefore not contain any known bugs that are planned to be fixed in the final release.
- With the pre-release, the release shepherd is responsible for coordinating or running the release candidate in any end user production environment for at least 1 week. This is typically done at Grafana Labs.
- If regressions or critical bugs are detected, they need to get fixed before cutting a new pre-release (called `-rc.1`, `-rc.2`, etc.).

See the next section for details on cutting an individual release.

## How to cut an individual release

### Branch management and versioning strategy

We use [Semantic Versioning](https://semver.org/).

We maintain a separate branch for each minor release, named `release-<major>.<minor>`, e.g. `release-1.1`, `release-2.0`.

The usual flow is to merge new features and changes into the main branch and to merge bug fixes into the latest release branch. Bug fixes are then merged into main from the latest release branch. The main branch should always contain all commits from the latest release branch. As long as main hasn't deviated significantly from the release branch, new commits can also go to main, followed by cherry picking them back into the release branch.

Maintaining the release branches for older minor releases happens on a best effort basis.

### Show that a release is in progress

This helps ongoing PRs to get their changes in the right place, and to consider whether they need cherry-picking.

1. Make a PR to update `CHANGELOG.md` on main
   - Add a new section for the new release so that `## main / unreleased` is blank and at the top.
   - The new section should say `## x.y.0-rc.0`.
1. Get this PR reviewed and merged.
1. Comment on open PRs with a CHANGELOG entry to rebase on `main` and move the CHANGELOG entry to the top under `## main / unreleased`

### Prepare your release

For a new major or minor release, create the corresponding release branch based on the main branch. For a patch release, work in the branch of the minor release you want to patch.

To prepare a release branch, first create new release branch (release-X.Y) in the Mimir repository from the main commit of your choice, and then do the following steps on a temporary branch (prepare-release-X.Y) and make a PR to merge said branch into the new release branch (prepare-release-X.Y -> release-X.Y):

1. Make sure you've a GPG key associated with your GitHub account (`git tag` will be signed with that GPG key)
   - You can add a GPG key to your GitHub account following [this procedure](https://help.github.com/articles/generating-a-gpg-key/)
1. Update the version number in the `VERSION` file to say "X.Y-rc.0"
1. Update `CHANGELOG.md`
   - Ensure changelog entries for the new release are in this order:
     - `[CHANGE]`
     - `[FEATURE]`
     - `[ENHANCEMENT]`
     - `[BUGFIX]`
   - Run `./tools/release/check-changelog.sh LAST-RELEASE-TAG...main` and add any missing PR which includes user-facing changes

Once your release preparation PR is approved, merge it to the "release-X.Y" branch, and continue with publishing.

### Publish a release candidate

To publish a release candidate:

1. Do not change the release branch directly; make a PR to the release-X.Y branch with VERSION and any CHANGELOG changes.
   1. Ensure the `VERSION` file has the `-rc.X` suffix (`X` starting from `0`)
1. After merging your PR to the release branch, `git tag` the new release (see [How to tag a release](#how-to-tag-a-release)) from the release branch.
1. Wait until the CI pipeline succeeds (once a tag is created, the release process through GitHub Actions will be triggered for this tag)
1. Create a pre-release on GitHub
   - Write the release notes (including a copy-paste of the changelog)
   - Build binaries with `make BUILD_IN_CONTAINER=true dist` and attach them to the release (building in container ensures standardized toolchain)

### Publish a stable release

> **Note:** Technical documentation is automatically published on release tags or release branches with a corresponding release tag. The workflow that publishes documentation is defined in [`publish-technical-documentation-release.yml`](.github/workflows/publish-technical-documentation-release.yml).
> To publish a stable release:

1. Do not change the release branch directly; make a PR to the release-X.Y branch with VERSION and any CHANGELOG changes.
   1. Ensure the `VERSION` file has **no** `-rc.X` suffix
   1. Update the Mimir version in the following locations:
      - `operations/mimir/images.libsonnet` (`_images.mimir` and `_images.query_tee` fields)
      - `operations/mimir-rules-action/Dockerfile` (`grafana/mimirtool` image tag)
1. Update dashboard screenshots
   1. Run `make mixin-screenshots`
   1. Review all updated screenshots and ensure no sensitive data is disclosed
   1. Open a PR
1. After merging your PR to the release branch, `git tag` the new release (see [How to tag a release](#how-to-tag-a-release)) from the release branch.
1. Wait until the CI pipeline succeeds (once a tag is created, the release process through GitHub Actions will be triggered for this tag)
1. Create a release on GitHub
   - Write the release notes (including a copy-paste of the changelog)
   - Build binaries with `make BUILD_IN_CONTAINER=true dist` and attach them to the release (building in container ensures standardized toolchain)
1. Merge the release branch `release-x.y` into `main`
   - Create `merge-release-X.Y-to-main` branch **from the `release-X.Y` branch** locally
   - Merge the upstream `main` branch into your `merge-release-X.Y-to-main` branch and resolve conflicts
   - Make a PR for merging your `merge-release-X.Y-to-main` branch into `main`
   - Once approved, merge the PR with a "Merge" commit through one of the following strategies:
     - Temporarily enable "Allow merge commits" option in "Settings > Options"
     - Locally merge the `merge-release-X.Y-to-main` branch into `main`, and push the changes to `main` back to GitHub. This doesn't break `main` branch protection, since the PR has been approved already, and it also doesn't require removing the protection.
1. Open a PR to add the new version to the backward compatibility integration test (`integration/backward_compatibility_test.go`)
1. Publish dashboards (done by a Grafana Labs member)
   1. Login to [https://grafana.com](https://grafana.com) with your Grafana Labs account
   1. Open [https://grafana.com/orgs/grafana/dashboards](https://grafana.com/orgs/grafana/dashboards)
   1. For each dashboard at `operations/mimir-mixin-compiled/dashboards`:
      1. Open the respective dashboard page
      1. Click "Revisions" tab
      1. Click "Upload new revision" and upload the updated `.json`

### How to tag a release

Every release is tagged with `mimir-<major>.<minor>.<patch>`, e.g. `mimir-2.0.0`. Note the `mimir-` prefix, which we use to specifically avoid the Go compiler recognizing them as version tags. We don't want compatibility with Go's module versioning scheme, since it would require us to keep each major version's code in its own directory beneath the repository root, f.ex. v2/. We also don't provide any API backwards compatibility guarantees within a single major version.

You can do the tagging on the commandline:

```bash
$ version=$(< VERSION)
$ git tag -s "mimir-${version}" -m "v${version}"
$ git push origin "mimir-${version}"
```
