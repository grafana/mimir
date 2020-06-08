# Changelog

## unreleased / master

## v0.2.1 / 2020-06-08

* [FEATURE] Add `rules check` command. It runs various [best practice](https://prometheus.io/docs/practices/rules/) checks against rules.
* [ENHANCEMENT] Ensure `rules prepare` takes into account Vector Matching on PromQL Binary Expressions.

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
