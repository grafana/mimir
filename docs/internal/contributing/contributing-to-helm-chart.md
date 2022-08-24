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

Update the Helm repository to refresh the list of available charts, by using the command `helm repo update`.

In order to search, template, install, upgrade, etc beta versions of charts, Helm commands require the user to specify the `--devel` flag. This means that checking for whether the beta version is published should be done with `helm search repo --devel`.

## Linting

Run [ct](https://github.com/helm/chart-testing) with the `docker` command:

```bash
docker run --rm -u $(id -g):$(id -u) -e HOME=/tmp -v $(pwd):/data quay.io/helmpack/chart-testing:latest sh -c "ct lint --all --debug --chart-dirs /data/operations/helm/charts --check-version-increment false --config /data/operations/helm/ct.yaml"
```

## Automated comparison with Jsonnet

In order to prevent configuration drift between the Mimir jsonnet library and the Mimir helm chart, an automated diff is performed against every pull request.
This diff makes extensive use of [kustomize](https://kustomize.io) to remove unimportant or known differences between the two sets of manifests.
A custom kustomize function is used to extract the Mimir configuration from kubernetes manifests so that it can also be compared.
The end goal is to ensure that only "useful" differences appear in the diff output.
Deciding which differences are useful is a complicated topic, but at a high level we use the following heuristics:

- Differences in Kubernetes annotations and labels are typically not useful, since changes in those will appear in other tests (ie golden record, functionality, etc).
- Differences in configuration parameters related to urls and file paths are typically not interesting, since they don't change _what_ the cluster does, only _where_ it happens.
- Differences in performance or scale related properties are typically useful, since these often have difficult-to-test implications on the cluster.

In order to keep the kustomize configuration manageable, it is divided into overlays, each named based on the order they are applied.

For example, at the time of writing, the Helm chart is passed through the following overlays:

```
$ ls -1 operations/compare-helm-with-jsonnet/helm
00-base
01-ignore
02-configs-and-k8s-defaults
03-set-namespace
04-labels
05-memberlist
06-memcached
07-services
08-pods
09-config
```

Each directory contains a `kustomize.yaml` file that describes the transformations made in that overlay.
A full explanation of kustomize is outside the scope of this document, but generally the overlays do one or all of the following:

1. Remove properties that are not useful for diffing
2. Modify property values so that they match between Helm and Jsonnet
3. Remove entire objects that only exist in either Helm or Jsonnet

### make check-helm-jsonnet-diff

You can use the `make check-helm-jsonnet-diff` target to perform an automatic diff of the Helm and Jsonnet templates.
This target requires the following:

- `yq`, `kubectl`, and `kustomize` installed and on the PATH
- A running kubernetes API server selected as the currently active `kubectl` context.

The API server is only used to perform dry-runs of server-side apply.
No resources are actually created in kubernetes, but API calls will be made against the server.
It is recommended to use kind, k3d, or some other local development cluster.
This allows the final diff to ignore any fields that match kubernetes defaults.
Ideally, there will be no differences between helm and jsonnet, so it's expected that the kustomize configuration will shrink over time rather than grow.
Achieving perfect parity is difficult, so this process allows us to incrementally fix differences while also preventing new differences from being added.

If CI reports a difference, you have several options:

1. Modify the Helm chart or Jsonnet library such that the differences are no longer present
2. Modify the Helm values file or Jsonnet configuration such that the differences are no longer present
3. If the difference is not useful, modify the kustomize configuration to remove that field or otherwise patch the manifests such that the differences are no longer present

### operations/compare-helm-with-jsonnet/compare-kustomize-outputs.sh

There is another script, `operations/compare-helm-with-jsonnet/compare-kustomize-outputs.sh`, which can be used to compare any two overlays in the diffing process.
This can be a useful tool when debugging kustomize configuration.

For example, the following invocation will show only changes that have been applied by the 9th helm overlay.

```
cd operations/compare-helm-with-jsonnet
./compare-kustomize-outputs.sh ./helm/08-* ./helm/09-*
```

The output can be filtered further by supplying a [yq](https://mikefarah.gitbook.io/yq/operators/select) `select` expression.
This is useful to limit otherwise noisy output to only show objects of a certain kind, name, or other property.

```
cd operations/compare-helm-with-jsonnet
./compare-kustomize-outputs.sh ./helm/09-* ./jsonnet/09-* 'select(.kind == "StatefulSet")'
```

### Static checks

Use the make targets `conftest-fmt`, `conftest-verify` and `conftest-test` to lint, verify and execute [conftest](https://www.conftest.dev/) static analysis tests.

The tests are verifying that the policies defined in `operations/helm/policies` are met for all Kubernetes manifests generated from configurations defined under `operations/helm/charts/mimir-distributed/ci`.
