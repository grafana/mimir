# Notes on testing mimir-distributed

These instructions are for testing the chart before Mimir release.

## Pulling mimir image

Mimir image is stored in a private Docker Hub repository, so in order to pull it, first we'll need to create a secret within our test cluster containing our docker credentials:

```sh
kubectl create secret docker-registry <your-secret-name> --docker-username <your-docker-user> --docker-password <your-docker-password> --docker-email <your-docker-email>
```

Next, specify the name of the recently created secret under the `image.pullSecrets` key in you custom values file, or on the command line.

```sh
helm install ... --set 'image.pullSecrets={<your-secret-name>}'
```

## Install instructions

While the chart is in development and in a private repository, it is more useful to install from files instead of Helm repo feature.

Once the repository is checked out and correct branch is set, let helm download dependencies (i.e. sub-charts):

```sh
cd charts/mimir-distributed
helm repo add helm.min.io https://helm.min.io
helm repo add bitnami https://charts.bitnami.com/bitnami
helm dependency update
cd ../..
```

Next, you can customize the chart using additional values and install it:

```sh
helm install <name-of-cluster> ./charts/mimir-distributed/
# or
helm install <name-of-cluster> ./charts/mimir-distributed/ -f <path-to-your-values-file>
# or
helm install <name-of-cluster> ./charts/mimir-distributed/ -f ci/test-values.yaml
# to turn off persistency - untested
```

## Upgrade

Upgrade if changes are made can be installed with
```sh
helm upgrade <name-of-cluster> ./charts/mimir-distributed/
```
Supply the same arguments as in install, but use the command `upgrade`.

## Diff
Install helm diff plugin so you can do
```sh
helm diff upgrade <name-of-cluster> ./charts/mimir-distributed/
```
Which is pretty much the same as `tk diff`.

## Remove
To delete the whole thing (wait a minute for stuff to terminate):
```sh
helm delete <name-of-cluster>
```

## Migration from Cortex

Untested. In theory: take the Cortex configuration yaml and go through it's migration. Set the config in the value `mimir.config`.

_Note_ that the value should be a string, not structured data. On the other hand, helm template expressions can be used in the value, see the default in `values.yaml` for inspiration.
