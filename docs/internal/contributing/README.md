Welcome! We're excited that you're interested in contributing. Below are some basic guidelines.

## Workflow

Grafana Mimir follows a standard GitHub pull request workflow. If you're unfamiliar with this workflow, read the very helpful [Understanding the GitHub flow](https://guides.github.com/introduction/flow/) guide from GitHub.

You are welcome to create draft PRs at any stage of readiness.

Doing so can be a helpful way of asking for assistance or to develop an idea.

When you open a PR as a draft, add a short description of what you’re still working on, what you are seeking assistance with, or both.

There is an automated GitHub action which closes PRs after 180 days of inactivity to keep the PR list clean. 30 days before closing, the GitHub action will add `stale` label to the PR. If you need more time, please remove the `stale` label.

Before a piece of work is finished:

- Organize it into one or more commits, and include a commit message for each that describes all of the changes that you made in that commit. It is more helpful to explain _why_ more than _what_, which are available via `git diff`.
- Each commit should build towards the whole - don't leave in back-tracks and mistakes that you later corrected.
- Have unit and/or [integration](./how-integration-tests-work.md) tests for new functionality or tests that would have caught the bug being fixed.
- Include a [CHANGELOG](#changelog) message if users of Grafana Mimir need to hear about what you did.
- If you have made any changes to flags or config, run `make doc` and commit the changed files to update the config file documentation.

## Grafana Mimir Helm chart

Please see the dedicated "[Contributing to Grafana Mimir helm chart](contributing-to-helm-chart.md)" page.

## Formatting

Grafana Mimir uses `goimports` tool (`go get golang.org/x/tools/cmd/goimports` to install) to format the Go files, and sort imports. We use goimports with `-local github.com/grafana/mimir` parameter, to put Grafana Mimir internal imports into a separate group. We try to keep imports sorted into three groups: imports from standard library, imports of 3rd party packages and internal Grafana Mimir imports. Goimports will fix the order, but will keep existing newlines between imports in the groups. We try to avoid extra newlines like that.

You're using an IDE you may find useful the following settings for the Grafana Mimir project:

- [VSCode](vscode-goimports-settings.json)

## Building Grafana Mimir

To build:

```
make
```

You can use `make help` to see the available targets.
(By default, the build runs in a Docker container, using an image built
with all the tools required. The source code is mounted from where you
run `make` into the build container as a Docker volume.
The mount options can be adjusted with `CONTAINER_MOUNT_OPTIONS`.)

To run the unit tests suite:

```
go test ./...
```

To run the integration tests suite please see "[How integration tests work](./how-integration-tests-work.md)".

If using macOS, make sure you have `gnu-sed` installed; otherwise, some make targets will not work properly.

Depending on how Docker is installed and configured and also the hardening applied to your workstation, using the Docker mount options might not work properly.
This is also true if you are using an alternative to Docker like Podman. In such case, you can use `CONTAINER_MOUNT_OPTIONS` to adjust the mount option.

Example:

```
make CONTAINER_MOUNT_OPTIONS=delegated
```

### Run Grafana Mimir locally

To easily run Grafana Mimir locally during development, use the docker-compose setup at `development/<deployment-mode>/`:

```bash
development/mimir-read-write-mode/compose-up.sh -d
```

Doing so builds the local version of the code and starts the Mimir components that are part of the chosen deployment mode as a set of containers. Use the `compose-down.sh` script to tear down the containers when you no longer need them.

### Dependency management

We uses [Go modules](https://golang.org/cmd/go/#hdr-Modules__module_versions__and_more) to manage dependencies on external packages.
This requires a working Go environment with version 1.11 or greater, git and [bzr](http://wiki.bazaar.canonical.com/Download) installed.

To add or update a new dependency, use the `go get` command:

```bash
# Pick the latest tagged release.
go get example.com/some/module/pkg

# Pick a specific version.
go get example.com/some/module/pkg@vX.Y.Z
```

Tidy up the `go.mod` and `go.sum` files:

```bash
go mod tidy
go mod vendor
git add go.mod go.sum vendor
git commit
```

You have to commit the changes to `go.mod` and `go.sum` before submitting the pull request.

## Design patterns and Code conventions

Please see the dedicated "[Design patterns and Code conventions](design-patterns-and-conventions.md)" page.

## Documentation

The Grafana Mimir documentation and the Helm chart _documentation_ for Mimir and GEM are compiled and published to [https://grafana.com/docs/mimir/latest/](https://grafana.com/docs/mimir/latest/) and [https://grafana.com/docs/helm-charts/mimir-distributed/latest/](https://grafana.com/docs/helm-charts/mimir-distributed/latest/). Run `make docs` to build and serve the documentation locally.
For more detail on style and organisation of the documentation, refer to the dedicated page "[How to write documentation](how-to-write-documentation.md)".

Note: if you attempt to view pages on GitHub, it's likely that you might find broken links or pages. That is expected and should not be addressed unless it is causing issues with the site that occur as part of the build.

## Errors catalog

We document the common user-visible errors so it is easy for the user to search for how to address those errors when they see them.

To add a new error:

- Under `pkg/util/globalerror/user.go`, create a new unique ID string as a constant. After your changes make it into a public release, do not change this string.
- When returning the error, use one of the functions in `globalerror` to generate the message. If you return the same error from multiple places, create a new function to return that error so that its message string is defined in only one place. Then, add a simple test for that function to compare its actual output with the expected message which is defined as a hard-coded string.
- Update the runbook in `docs/sources/mimir/manage/mimir-runbooks/_index.md` with details about why the error happens, and if possible how to address it.

## Changelog

When appending to the changelog, the changes must be listed with a corresponding scope. A scope denotes the type of change that has occurred.

The ordering of entries in the changelog should be `[CHANGE]`, `[FEATURE]`, `[ENHANCEMENT]`, `[BUGFIX]`.

#### [CHANGE]

The CHANGE scope denotes a change that changes the expected behavior of the project while not adding new functionality or fixing an underling issue. This commonly occurs when renaming things to make them more consistent or to accommodate updated versions of vendored dependencies.

#### [FEATURE]

The FEATURE scope denotes a change that adds new functionality to the project/service.

#### [ENHANCEMENT]

The ENHANCEMENT scope denotes a change that improves upon the current functionality of the project/service. Generally, an enhancement is something that improves upon something that is already present. Either by making it simpler, more powerful, or more performant. For example:

- An optimization on a particular process in a service that makes it more performant
- Simpler syntax for setting a configuration value, like allowing `1m` instead of 60 for a duration setting.

#### [BUGFIX]

The BUGFIX scope denotes a change that fixes an issue with the project in question. A BUGFIX should align the behaviour of the service with the current expected behaviour of the service. If a BUGFIX introduces new unexpected behaviour to ameliorate the issue, a corresponding FEATURE or ENHANCEMENT scope should also be added to the changelog.
