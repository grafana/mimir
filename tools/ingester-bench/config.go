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

// benchConfig is how the ingesters run: flags of all, then of each, over the ones the bench sets for its own needs. A
// and B always run; C and D too when their configuration sets anything, to compare more than two at once.
type benchConfig struct {
	Common ingesterSpec `yaml:"common" json:"common"`
	A      ingesterSpec `yaml:"a" json:"a"`
	B      ingesterSpec `yaml:"b" json:"b"`
	C      ingesterSpec `yaml:"c,omitempty" json:"c,omitzero"`
	D      ingesterSpec `yaml:"d,omitempty" json:"d,omitzero"`
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
	return cfg, cfg.validate()
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
	c.C.merge(layer.C)
	c.D.merge(layer.D)
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

// ingesters is how many ingesters run: A, B, and C and D when they're configured, D only after C.
func (c benchConfig) ingesters() int {
	switch {
	case !c.C.isZero() && !c.D.isZero():
		return 4
	case !c.C.isZero():
		return 3
	default:
		return 2
	}
}

// validate rejects a D without a C, which would otherwise be left out without a word.
func (c benchConfig) validate() error {
	if c.C.isZero() && !c.D.isZero() {
		return errors.New("ingester d is configured without ingester c")
	}
	return nil
}

// ingester is the spec of ingester i, 0 for A, 1 for B and so on: its own over the common one.
func (c benchConfig) ingester(i int) ingesterSpec {
	var s ingesterSpec
	s.merge(c.Common)
	s.merge([]ingesterSpec{c.A, c.B, c.C, c.D}[i])
	return s
}

func (s ingesterSpec) isZero() bool {
	return s.Name == "" && s.Binary == "" && s.Image == "" && len(s.Args) == 0
}

// withArgs is the flags of args with the spec's over them: a value replaces a flag's in place, nil removes it, and
// flags args doesn't have come after, by name.
func (s ingesterSpec) withArgs(args []string) []string {
	out := make([]string, 0, len(args))
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
