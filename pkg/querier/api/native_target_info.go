// SPDX-License-Identifier: AGPL-3.0-only

package api

// NativeTargetInfoHeader picks, per request, how info() finds target_info in the native metadata prototype: "native",
// from the ingesters' native resources, which is what tenants with native resources get without it, or "join", from
// the stored target_info series.
const NativeTargetInfoHeader = "X-Mimir-Native-Target-Info"
