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

func (a *Annotations) Decode() annotations.Annotations {
	if len(a.Infos) == 0 && len(a.Warnings) == 0 {
		return nil
	}

	annos := make(annotations.Annotations, len(a.Infos)+len(a.Warnings))

	for _, a := range a.Infos {
		annos.Add(NewInfoAnnotation(a))
	}

	for _, a := range a.Warnings {
		annos.Add(NewWarningAnnotation(a))
	}

	return annos
}

func EncodeAnnotations(a annotations.Annotations, originalExpression string) Annotations {
	if len(a) == 0 {
		return Annotations{}
	}

	warnings, infos := a.AsStrings(originalExpression, 0, 0)

	return Annotations{
		Warnings: warnings,
		Infos:    infos,
	}
}
