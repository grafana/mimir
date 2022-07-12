// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/tsdb/fileutil/mmap_386.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

//go:build windows
// +build windows

package fileutil

const maxMapSize = 0x7FFFFFFF // 2GB
