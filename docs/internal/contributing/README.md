Welcome! We're excited that you're interested in contributing. Below are some basic guidelines.

## Workflow

Grafana Mimir follows a standard GitHub pull request workflow. If you're unfamiliar with this workflow, read the very helpful [Understanding the GitHub flow](https://guides.github.com/introduction/flow/) guide from GitHub.

You are welcome to create draft PRs at any stage of readiness - this
can be helpful to ask for assistance or to develop an idea. But before
a piece of work is finished it should:

- Be organised into one or more commits, each of which has a commit message that describes all changes made in that commit ('why' more than 'what' - we can read the diffs to see the code that changed).
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

(By default, the build runs in a Docker container, using an image built
with all the tools required. The source code is mounted from where you
run `make` into the build container as a Docker volume.)

To run the unit tests suite:

```
go test ./...
```

To run the integration tests suite please see "[How integration tests work](./how-integration-tests-work.md)".

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

The Grafana Mimir documentation is compiled into a website published at [grafana.com](https://grafana.com/). Please see "[How to run the website locally](./how-to-run-website-locally.md)" for instructions.

Note: if you attempt to view pages on Github, it's likely that you might find broken links or pages. That is expected and should not be addressed unless it is causing issues with the site that occur as part of the build.

## Changelog

When appending to the changelog, the changes must be listed with a corresponding scope. A scope denotes the type of change that has occurred.

The ordering of entries in the changelog should be `[CHANGE]`, `[FEATURE]`, `[ENHANCEMENT]`, `[BUGFIX]`.

#### [CHANGE]

The CHANGE scope denotes a change that changes the expected behavior of the project while not adding new functionality or fixing an underling issue. This commonly occurs when renaming things to make them more consistent or to accommodate updated versions of vendored dependencies.

#### [FEATURE]

The FEATURE scope denotes a change that adds new functionality to the project/service.

#### [ENHANCEMENT]

The ENHANCEMENT scope denotes a change that improves upon the current functionality of the project/service. Generally, an enhancement is something that improves upon something that is already present. Either by making it simpler, more powerful, or more performant. For Example:

- An optimization on a particular process in a service that makes it more performant
- Simpler syntax for setting a configuration value, like allowing `1m` instead of 60 for a duration setting.

#### [BUGFIX]

The BUGFIX scope denotes a change that fixes an issue with the project in question. A BUGFIX should align the behaviour of the service with the current expected behaviour of the service. If a BUGFIX introduces new unexpected behaviour to ameliorate the issue, a corresponding FEATURE or ENHANCEMENT scope should also be added to the changelog.
