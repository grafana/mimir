//go:build tools
// +build tools

package csproto

import (
	_ "github.com/gogo/protobuf/protoc-gen-gogo"
	_ "google.golang.org/protobuf/cmd/protoc-gen-go"
)
