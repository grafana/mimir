// Copyright 2025 The go-yaml Project Contributors
// SPDX-License-Identifier: Apache-2.0

// Package libyaml implements go-yaml's internal load and dump pipelines.
// It combines libyaml-derived scanning, parsing, and emitting with node
// composition, tag resolution, Go value construction, and representation.
// Applications should use github.com/pb33f/go-yaml rather than this internal package.
package libyaml
