// Copyright 2013 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package model

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"
)

// ValidationScheme is a Go enum for determining how metric and label names will
// be validated by this library.
type ValidationScheme int

const (
	// LegacyValidation is a setting that requirets that metric and label names
	// conform to the original Prometheus character requirements described by
	// MetricNameRE and LabelNameRE.
	LegacyValidation ValidationScheme = iota

	// UTF8Validation only requires that metric and label names be valid UTF8
	// strings.
	UTF8Validation
)

var (
	// NameValidationScheme determines the method of name validation to be used by
	// all calls to IsValidMetricName() and LabelName IsValid(). Setting UTF8 mode
	// in isolation from other components that don't support UTF8 may result in
	// bugs or other undefined behavior. This value is intended to be set by
	// UTF8-aware binaries as part of their startup. To avoid need for locking,
	// this value should be set once, ideally in an init(), before multiple
	// goroutines are started.
	NameValidationScheme = LegacyValidation

	// MetricNameRE is a regular expression matching valid metric
	// names. Note that the IsValidMetricName function performs the same
	// check but faster than a match with this regular expression.
	MetricNameRE = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)
)

// A Metric is similar to a LabelSet, but the key difference is that a Metric is
// a singleton and refers to one and only one stream of samples.
type Metric LabelSet

// Equal compares the metrics.
func (m Metric) Equal(o Metric) bool {
	return LabelSet(m).Equal(LabelSet(o))
}

// Before compares the metrics' underlying label sets.
func (m Metric) Before(o Metric) bool {
	return LabelSet(m).Before(LabelSet(o))
}

// Clone returns a copy of the Metric.
func (m Metric) Clone() Metric {
	clone := make(Metric, len(m))
	for k, v := range m {
		clone[k] = v
	}
	return clone
}

func (m Metric) String() string {
	metricName, hasName := m[MetricNameLabel]
	numLabels := len(m) - 1
	if !hasName {
		numLabels = len(m)
	}
	labelStrings := make([]string, 0, numLabels)
	for label, value := range m {
		if label != MetricNameLabel {
			labelStrings = append(labelStrings, fmt.Sprintf("%s=%q", label, value))
		}
	}

	switch numLabels {
	case 0:
		if hasName {
			return string(metricName)
		}
		return "{}"
	default:
		sort.Strings(labelStrings)
		return fmt.Sprintf("%s{%s}", metricName, strings.Join(labelStrings, ", "))
	}
}

// Fingerprint returns a Metric's Fingerprint.
func (m Metric) Fingerprint() Fingerprint {
	return LabelSet(m).Fingerprint()
}

// FastFingerprint returns a Metric's Fingerprint calculated by a faster hashing
// algorithm, which is, however, more susceptible to hash collisions.
func (m Metric) FastFingerprint() Fingerprint {
	return LabelSet(m).FastFingerprint()
}

// IsValidMetricName returns true iff name matches the pattern of MetricNameRE
// for legacy names, and iff it's valid UTF-8 if the UTF8Validation scheme is
// selected.
func IsValidMetricName(n LabelValue) bool {
	switch NameValidationScheme {
	case LegacyValidation:
		return IsValidLegacyMetricName(n)
	case UTF8Validation:
		if len(n) == 0 {
			return false
		}
		return utf8.ValidString(string(n))
	default:
		panic(fmt.Sprintf("Invalid name validation scheme requested: %d", NameValidationScheme))
	}
}

// IsValidLegacyMetricName is similar to IsValidMetricName but always uses the
// legacy validation scheme regardless of the value of NameValidationScheme.
// This function, however, does not use MetricNameRE for the check but a much
// faster hardcoded implementation.
func IsValidLegacyMetricName(n LabelValue) bool {
	if len(n) == 0 {
		return false
	}
	for i, b := range n {
		if !((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || b == '_' || b == ':' || (b >= '0' && b <= '9' && i > 0)) {
			return false
		}
	}
	return true
}
