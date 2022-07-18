// SPDX-License-Identifier: AGPL-3.0-only

/*
Package minisdk contains a minified version of the model found in github.com/grafana-tools/sdk.
Most of the model parts were stripped of to reduce the unmarshaling issues caused by model evolution,
as our objective is to just find the queries contained in the dashboards.

This model could now be probably simplified, but it's left in a form compatible with the original model,
so it would be easier to switch back if that's ever fixed.
*/
package minisdk
