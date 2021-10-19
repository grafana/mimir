# Monitoring for Mimir

To generate the Grafana dashboards and Prometheus alerts for Mimir:

## Usage

```console
$ GO111MODULE=on go get github.com/monitoring-mixins/mixtool/cmd/mixtool
$ GO111MODULE=on go get github.com/jsonnet-bundler/jsonnet-bundler/cmd/jb
$ git clone https://github.com/grafana/mimir.git
$ make build-mixin
```

This will leave all the alerts and dashboards in jsonnet/mimir-mixin/mimir-mixin.zip (or jsonnet/mimir-mixin/out).

## Known Problems

If you get an error like `cannot use cli.StringSliceFlag literal (type cli.StringSliceFlag) as type cli.Flag in slice literal` when installing [mixtool](https://github.com/monitoring-mixins/mixtool/issues/27), make sure you set `GO111MODULE=on` before `go get`.
