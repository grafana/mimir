# Contributing to the Grafana Mimir Helm Chart

## Differences to general workflow

Please see the [general workflow](README.md#workflow) for reference.

- Changelog is in the chart itself [operations/helm/charts/mimir-distributed/CHANGELOG.md](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/CHANGELOG.md).
- If you made any changes to the [operations/helm/charts/mimir-distributed/Chart.yaml](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/Chart.yaml), run `make doc` and commit the changed files to update the [operations/helm/charts/mimir-distributed/README.md](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/README.md).
- If your changes impact the test configurations in the [operations/helm/charts/mimir-distributed/ci](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/ci) directory, see [Updating compiled manifests](#updating-compiled-manifests).

## Updating compiled manifests

We keep a compiled version of the helm chart for each values file in the `ci` directory.
This makes it easy to see how a given PR impacts the final output.
A PR check will fail if you forget to update the compiled manifests, and you can use `make build-helm-tests` to update them.

## Versioning

Normally contributors need _not_ bump the version. The chart will be released with a beta version weekly by maintainers (unless no changes were made) and also regular stable releases will be released by cherry picking commits from the `main` branch to a release branch (e.g. `release-2.1`).

If version increase is need, the version is set in the chart itself [operations/helm/charts/mimir-distributed/Chart.yaml](https://github.com/grafana/mimir/blob/main/operations/helm/charts/mimir-distributed/Chart.yaml). On the `main` branch, versions should be _pre-release_ only, meaning that the version should be suffixed by `-beta.<n>`, for example `2.1.0-beta.1` for a pre-release of the next stable `2.1.0` chart version. Versioning should follow the [Helm3 standard](https://helm.sh/docs/topics/charts/#charts-and-versioning) which is [SemVer 2](https://semver.org/spec/v2.0.0.html).

## Using beta version

Once a PR that updates the chart version is merged to `main`, it takes a couple of minutes for it to be published in [https://grafana.github.io/helm-charts](https://grafana.github.io/helm-charts) Helm repository.

In order to search, template, install, upgrade, etc beta versions of charts, Helm commands require the user to specify the `--devel` flag. This means that checking for whether the beta version is published should be done with `helm search repo --devel`.
