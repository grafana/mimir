// SPDX-License-Identifier: AGPL-3.0-only

/*
Package minisdk contains a minified version of the model and REST client found in github.com/grafana-tools/sdk.
Most of the model parts were stripped of to reduce the unmarshaling issues caused by model evolution,
as our objective is to just find the queries contained in the dashboards.

The client implements only the subset of the Grafana REST API used by mimirtool (dashboard search and
raw dashboard fetch). The model is left in a form compatible with the original sdk, so it would be
easier to switch back if its unmarshaling issues are ever fixed.
*/
package minisdk
