// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"

	"go.yaml.in/yaml/v3"
)

//go:embed defaults.yaml
var defaultsYAML []byte

// benchConfig is how the ingesters run: flags of both, then of each, over the ones the bench sets for its own needs.
type benchConfig struct {
	Common ingesterSpec `yaml:"common" json:"common"`
	A      ingesterSpec `yaml:"a" json:"a"`
	B      ingesterSpec `yaml:"b" json:"b"`
}

type ingesterSpec struct {
	Name   string `yaml:"name,omitempty" json:"name,omitempty"`
	Binary string `yaml:"binary,omitempty" json:"binary,omitempty"`
	Image  string `yaml:"image,omitempty" json:"image,omitempty"`
	// Flags without their dash, and their values: nil removes a flag a layer below set.
	Args map[string]*string `yaml:"args,omitempty" json:"args,omitempty"`
}

// loadConfig is the defaults with the files over them, each over the ones before.
func loadConfig(paths []string) (benchConfig, error) {
	var cfg benchConfig
	if err := cfg.merge(defaultsYAML); err != nil {
		return cfg, fmt.Errorf("defaults: %w", err)
	}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return cfg, err
		}
		if err := cfg.merge(data); err != nil {
			return cfg, fmt.Errorf("%s: %w", path, err)
		}
	}
	return cfg, nil
}

func (c *benchConfig) merge(data []byte) error {
	var layer benchConfig
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&layer); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	c.Common.merge(layer.Common)
	c.A.merge(layer.A)
	c.B.merge(layer.B)
	return nil
}

func (s *ingesterSpec) merge(layer ingesterSpec) {
	if layer.Name != "" {
		s.Name = layer.Name
	}
	if layer.Binary != "" {
		s.Binary = layer.Binary
	}
	if layer.Image != "" {
		s.Image = layer.Image
	}
	for name, value := range layer.Args {
		if s.Args == nil {
			s.Args = map[string]*string{}
		}
		s.Args[strings.TrimLeft(name, "-")] = value
	}
}

// ingester is the spec of ingester i, 0 for A and 1 for B: its own over the common one.
func (c benchConfig) ingester(i int) ingesterSpec {
	var s ingesterSpec
	s.merge(c.Common)
	s.merge([]ingesterSpec{c.A, c.B}[i])
	return s
}

// withArgs is the flags of args with the spec's over them: a value replaces a flag's in place, nil removes it, and
// flags args doesn't have come after, by name.
func (s ingesterSpec) withArgs(args []string) []string {
	out := make([]string, 0, len(args)+len(s.Args))
	set := map[string]bool{}
	for _, arg := range args {
		name, _, _ := strings.Cut(strings.TrimLeft(arg, "-"), "=")
		set[name] = true
		value, ok := s.Args[name]
		switch {
		case !ok:
			out = append(out, arg)
		case value != nil:
			out = append(out, "-"+name+"="+*value)
		}
	}
	var added []string
	for name, value := range s.Args {
		if !set[name] && value != nil {
			added = append(added, "-"+name+"="+*value)
		}
	}
	slices.Sort(added)
	return append(out, added...)
}
