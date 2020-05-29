# Changelog

## unreleased / master

* [FEATURE] Add `rules prepare` command. It allows you add a label to PromQL aggregations and lint your expressions in rule files.

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
