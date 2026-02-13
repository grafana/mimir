// SPDX-License-Identifier: AGPL-3.0-only

package querierpb

import "github.com/prometheus/prometheus/util/annotations"

// Prometheus' annotations.Annotations type stores Golang error types and checks if they
// are annotations.PromQLInfo or annotations.PromQLWarning, so this type allows us to
// create errors with arbitrary strings received from remote executors or retrieved from
// caches that satisfy this requirement.
type stringAnnotation struct {
	msg   string
	inner error
}

func NewWarningAnnotation(msg string) error {
	return &stringAnnotation{msg: msg, inner: annotations.PromQLWarning}
}

func NewInfoAnnotation(msg string) error {
	return &stringAnnotation{msg: msg, inner: annotations.PromQLInfo}
}

func (a *stringAnnotation) Error() string { return a.msg }
func (a *stringAnnotation) Unwrap() error { return a.inner }
