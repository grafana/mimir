// SPDX-License-Identifier: AGPL-3.0-only

//go:build !slicelabels && !dedupelabels

package activeseries

import (
	"unsafe"

	"github.com/prometheus/prometheus/model/labels"
)

// deletedSeriesLbls transmutes key (which comes from Labels.Bytes()) as
// labels.Labels. This is only correct under stringlabels.
func deletedSeriesLbls(key string, _ labels.Labels) labels.Labels {
	return *(*labels.Labels)(unsafe.Pointer(&key))
}

// Compile-time assert that sizeof(labels.Labels) == sizeof(string)
var (
	_ [unsafe.Sizeof(labels.Labels{}) - unsafe.Sizeof("")]byte
	_ [unsafe.Sizeof("") - unsafe.Sizeof(labels.Labels{})]byte
)

// Startup-time assert that Labels.Bytes just returns a copy of itself.
func init() {
	lbls := labels.FromMap(map[string]string{"foo": "bar", "baz": "qux"})
	lblsAsString := *(*string)(unsafe.Pointer(&lbls))
	lblsEncoded := string(lbls.Bytes(nil))
	if lblsAsString != lblsEncoded {
		panic("labels.Labels.Bytes no longer encodes as its own raw string")
	}
}
