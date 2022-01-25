# Changelog

Order should be `CHANGE`, `FEATURE`, `ENHANCEMENT`, and `BUGFIX`

## unreleased/master

* [FEATURE] Support Arm64 on Darwin for all binaries (benchtool etc). https://github.com/grafana/cortex-tools/pull/215
* [ENHANCEMENT] Correctly support federated rules. #823
* [BUGFIX] Fix `cortextool rules` legends displaying wrong symbols for updates and deletions. https://github.com/grafana/cortex-tools/pull/226
* [CHANGE] The following environment variables have been renamed:
  * `CORTEX_ADDRESS` to `MIMIR_ADDRESS`
  * `CORTEX_API_USER` to `MIMIR_API_USER`
  * `CORTEX_API_KEY` to `MIMIR_API_KEY`
  * `CORTEX_TENANT_ID` to `MIMIR_TENANT_ID`
  * `CORTEX_TLS_CA_PATH` to `MIMIR_TLS_CA_PATH`
  * `CORTEX_TLS_CERT_PATH` to `MIMIR_TLS_CERT_PATH`
  * `CORTEX_TLS_KEY_PATH` to `MIMIR_TLS_KEY_PATH`

## v0.10.7

* [ENHANCEMENT] Benchtool: add `-bench.write.proxy-url` argument for configuring the Prometheus remote-write client with a HTTP proxy URL. https://github.com/grafana/cortex-tools/pull/223
* [ENHANCEMENT] Analyse: support Grafana 'timeseries' panel type for `cortextool analyse grafana` command. https://github.com/grafana/cortex-tools/pull/224

## v0.10.6

* [FEATURE] Rules check for Loki now supports `pattern`.

## v0.10.5

* [ENHANCEMENT] Allow usage of HTTP_PROXY and HTTPS_PROXY. https://github.com/grafana/cortex-tools/pull/216

## v0.10.4

* [CHANGE] Update go image to v1.16.8. https://github.com/grafana/cortex-tools/pull/213
* [CHANGE] Update alpine image to v3.14. https://github.com/grafana/cortex-tools/pull/213
* [ENHANCEMENT] Add benchtool to the release process. https://github.com/grafana/cortex-tools/pull/213

## v0.10.3

* [BUGFIX] Fix `cortextool analyse grafana` failure on certain dashboards that use templating and/or panel heights due to unmarshalling errors with the underlying `grafana-tools/sdk` library. https://github.com/grafana/cortex-tools/pull/192

## v0.10.2

* [FEATURE] Blockgen: adding a new tool to generate blocks of mock data.
* [FEATURE] Support Arm64 on Darwin.
* [ENHANCEMENT] Added the ability to set an explicit user when Cortex is behind basic auth. https://github.com/grafana/cortex-tools/pull/187
* [BUGFIX] Benchtool: avoid duplicate DNS metrics registration when enabling both query and write benchmarking. https://github.com/grafana/cortex-tools/pull/188

## v0.10.1

* [ENHANCEMENT] `cortextool analyse prometheus` now records cardinality by metric and job labels. https://github.com/grafana/cortex-tools/pull/178

## v0.10.0

* [FEATURE] Add `--label-excluded-rule-groups` support to `cortextool rules prepare` command. https://github.com/grafana/cortex-tools/pull/174
* [ENHANCEMENT] Upgrade the Go version used in build images and tests to golang 1.16.3 to match upstream Cortex. https://github.com/grafana/cortex-tools/pull/165

## v0.9.0

* [CHANGE] Overrides Exporter: `cortex_overrides_presets` added to expose the preset metrics. https://github.com/grafana/cortex-tools/pull/154
  * `limit_type` label has been renamed to `limit_name`.
  * `type` label has been removed.
  * `cortex_overrides` now only exposes overrides and doesn't expose preset limits.
* [BUGFIX] Escape rule namespaces and groups containing slashes at the beginning and end of the name. https://github.com/grafana/cortex-tools/pull/162

## v0.8.0

* [CHANGE] Loadgen: Add `loadgen` namespace to loadgen metrics. https://github.com/grafana/cortex-tools/pull/152
  * `write_request_duration_seconds` --> `loadgen_write_request_duration_seconds`
  * `query_request_duration_seconds` --> `loadgen_query_request_duration_seconds`
* [FEATURE] Add `analyse` command to help you understand your metric usage. https://github.com/grafana/cortex-tools/pull/157 https://github.com/grafana/cortex-tools/pull/158
* [ENHANCEMENT] Return detailed HTTP error messages. https://github.com/grafana/cortex-tools/pull/146
* [ENHANCEMENT] Check for duplicate rule records in `cortextool rules check`. https://github.com/grafana/cortex-tools/pull/149
* [ENHANCEMENT] Loadgen: Metrics now use histogram with an additional `15` bucket.

## v0.7.2

* [ENHANCEMENT] Add format for rules list command. https://github.com/grafana/cortex-tools/pull/130
* [ENHANCEMENT] Parse multiple YAML rules documents. https://github.com/grafana/cortex-tools/pull/127
* [BUGFIX] Fix double escaping of special characters in rule and namespace names. https://github.com/grafana/cortex-tools/pull/140

## v0.7.1

* [BUGFIX] Rule commands use compatible routes with the Loki backend. https://github.com/grafana/cortex-tools/pull/136

## v0.7.0

* [FEATURE] Add `remote-read` commands to investigate series through the remote-read API. https://github.com/grafana/cortex-tools/pull/134
   - `remote-read export`: Export metrics remote read series into a local TSDB.
   - `remote-read dump`: Dump remote read series.
   - `remote-read stats`: Show statistic of remote read series.

## v0.6.1

* [BUGFIX] Fix `cortextool` generating the wrong paths when executing multiple calls to the cortex API. In particular, commands like `load`, `sync` were affected. https://github.com/grafana/cortex-tools/pull/133

## v0.6.0

* [CHANGE] When using `rules` commands, cortex ruler API requests will now default to using the `/api/v1/` prefix. The `--use-legacy-routes` flag has been added to allow users to use the original `/api/prom/` routes. https://github.com/grafana/cortex-tools/pull/99
* [FEATURE] Add support for position rule-files arguments to `rules sync` and `rules diff` https://github.com/grafana/cortex-tools/pull/125
* [FEATURE] Add an allow-list of namespaces for `rules sync` and `rules diff` https://github.com/grafana/cortex-tools/pull/125
* [ENHANCEMENT] Handle trailing slashes in URLs on `cortextool`. https://github.com/grafana/cortex-tools/pull/128
* [BUGFIX] Fix inaccuracy in `e2ealerting` caused by invalid purging condition on timestamps. https://github.com/grafana/cortex-tools/pull/117

## v0.5.0

* [FEATURE] Release `cortextool` via Homebrew for macOS https://github.com/grafana/cortex-tools/pull/109
* [FEATURE] Add new binary `e2ealerting` for measuring end to end alerting latency. https://github.com/grafana/cortex-tools/pull/110
* [BUGFIX] Do not panic if we're unable to contact GitHub for the `version` command. https://github.com/grafana/cortex-tools/pull/107

## v0.4.1

* [ENHANCEMENT] Upgrade the Go version used in build images and tests to golang 1.14.9 to match upstream Cortex. https://github.com/grafana/cortex-tools/pull/104
* [FEATURE] Add `chunktool chunk validate-index` and `chunktool chunk clean-index` commands to the chunktool. These commands are used to scan Cortex index backends for invalid index entries. https://github.com/grafana/cortex-tools/pull/104

## v0.4.0

* [ENHANCEMENT] Loadgen: Allow users to selectively disable query or write loadgen by leaving their respective URL configs empty. https://github.com/grafana/cortex-tools/pull/95
* [FEATURE] Add overrides-exporter to cortextool, which exports Cortex runtime configuration overrides as metrics. https://github.com/grafana/cortex-tools/pull/91

## v0.3.2

* [BUGFIX] Supports `rules lint` with LogQL: [https://github.com/grafana/cortex-tools/pull/92](https://github.com/grafana/cortex-tools/pull/92).

## v0.3.1

* [FEATURE] Add support for GME remote-write rule groups. https://github.com/grafana/cortex-tools/pull/82
* [BUGFIX] Fix issue where rule comparisons would be affected by the format of the YAML file. https://github.com/grafana/cortex-tools/pull/88

## v0.3.0

* [FEATURE] Added loki backend support for the rules commands, configurable with `--backend=loki` (defaults to cortex).
* [FEATURE] Introduces a new `version` command. The command will also let you know if there's a new version available.

## v0.2.4

* [BUGFIX] Fix alertmanager registration subcommand and path for the configuration API https://github.com/grafana/cortex-tools/pull/72

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

* [CHANGE] Ensure 404 deletes do not trigger an error for `rules` and `alertmanager` commands https://github.com/grafana/cortex-tools/pull/28
* [ENHANCEMENT] Add separate `chunktool` for operating on cortex chunk backends https://github.com/grafana/cortex-tools/pull/23 https://github.com/grafana/cortex-tools/pull/26
* [ENHANCEMENT] Add `--disable-color` flag to `rules print`, `rules diff`, and `alertmanager get` command https://github.com/grafana/cortex-tools/pull/25
* [BUGFIX] `alertmanager load` command will ensure provided template files are also loaded https://github.com/grafana/cortex-tools/pull/25
* [BUGFIX] Ensure rules commands use escaped URLs when contacting cortex API https://github.com/grafana/cortex-tools/pull/24

## v0.1.3 / 2020-02-04

* [FEATURE] Add `rules sync` command
* [FEATURE] Add `rules diff` command

## v0.1.2 / 2019-12-18

* [CHANGE] Info log when a resource does not exist instead of exiting fatally
* [FEATURE] Windows build during Makefile cross compilation
* [BUGFIX] Fix env var `CORTEX_TENTANT_ID` to `CORTEX_TENANT_ID` for rule commands
