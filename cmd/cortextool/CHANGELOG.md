# Changelog

## unreleased / master

## v0.2.4

* [BUGFIX] Fix alertmanager registration subcommand and path for the configuration API #72

## v0.2.3

* [FEATURE] Added `alerts verify` command, which can be used to compare `ALERTS` series in Cortex to verify if Prometheus and Cortex Ruler are firing the same alerts
* [BUGFIX] Renamed module from cortextool to cortex-tools to ensure `go get` works properly.
* [BUGFIX] When using `--disable-color` for `rules get`, it now actually prints rules instead of the bytes of the underlying string
* [ENHANCEMENT] Allow mutualTLS for Cortex API client for `rules` and `alertmanager` cmds with:
  - `--tls-ca-path` or `CORTEX_TLS_CA_PATH`
  - `--tls-cert-path` or `CORTEX_TLS_CERT_PATH`
  - `--tls-key-path` or `CORTEX_TLS_KEY_PATH`

## v0.2.2 / 2020-06-09

* [BUGFIX] Remove usage of alternate PromQL parser in `rules prepare lint`.
* [BUGFIX] `rules check` does not require an argument.

## v0.2.1 / 2020-06-08

* [FEATURE] Add `rules check` command. It runs various [best practice](https://prometheus.io/docs/practices/rules/) checks against rules.
* [ENHANCEMENT] Ensure `rules prepare` takes into account Vector Matching on PromQL Binary Expressions.
* [BUGFIX] `rules prepare` and `rules lint` do not require an argument.

## v0.2.0 / 2020-06-02

* [FEATURE] Add `rules prepare` command. It allows you add a label to PromQL aggregations and lint your expressions in rule files.
* [FEATURE] Add `rules lint` command. It lints, rules YAML and PromQL expression formatting within the rule file
* [FEATURE] Add `logtool` binary. It parses Loki/Cortex query-frontend logs and formats them for easy analysis.

## v0.1.4 / 2020-03-10

* [CHANGE] Ensure 404 deletes do not trigger an error for `rules` and `alertmanager` commands #28
* [ENHANCEMENT] Add separate `chunktool` for operating on cortex chunk backends #23 #26
* [ENHANCEMENT] Add `--disable-color` flag to `rules print`, `rules diff`, and `alertmanager get` command #25
* [BUGFIX] `alertmanager load` command will ensure provided template files are also loaded #25
* [BUGFIX] Ensure rules commands use escaped URLs when contacting cortex API #24

## v0.1.3 / 2020-02-04

* [FEATURE] Add `rules sync` command
* [FEATURE] Add `rules diff` command

## v0.1.2 / 2019-12-18

* [CHANGE] Info log when a resource does not exist instead of exiting fatally
* [FEATURE] Windows build during Makefile cross compilation
* [BUGFIX] Fix env var `CORTEX_TENTANT_ID` to `CORTEX_TENANT_ID` for rule commands
