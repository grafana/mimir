// Copyright 2025 The Prometheus Authors
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

package semconv

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type MetricID string

type SemanticMetricID string

func (id MetricID) semanticID() (_ SemanticMetricID, revision int) {
	parts := strings.Split(string(id), ".")
	if len(parts) == 1 {
		return SemanticMetricID(id), 0
	}

	var err error
	revision, err = strconv.Atoi(parts[len(parts)-1])
	if err != nil {
		return SemanticMetricID(id), 0
	}

	// Number, assume revision.
	return SemanticMetricID(strings.Join(parts[:len(parts)-1], ".")), revision
}

type Changelog struct {
	Version int `yaml:"version"`

	MetricsChangelog map[SemanticMetricID][]MetricChange `yaml:"metrics_changelog"`

	fetchTime time.Time
}

type MetricChange struct {
	Forward  MetricGroupDescription
	Backward MetricGroupDescription
}

// MetricGroupDescription represents a semconv metric group.
// NOTE(bwplotka): Only implementing fields that matter for querying.
type MetricGroupDescription struct {
	MetricName  string      `yaml:"metric_name"`
	Unit        string      `yaml:"unit"`
	ValuePromQL string      `yaml:"value_promql"`
	Attributes  []Attribute `yaml:"attributes"`
}

func (m MetricGroupDescription) DirectUnit() string {
	if strings.HasPrefix(m.Unit, "{") {
		return strings.Trim(m.Unit, "{}") + "s"
	}
	// TODO(bwplotka): Yolo, fix it.
	return m.Unit
}

type Attribute struct {
	Tag     string            `yaml:"tag"`
	Members []AttributeMember `yaml:"members"`
}

type AttributeMember struct {
	Value string `yaml:"value"`
}

type FetchSchema func(baseUrl string, labelNames []string, out *Schema) error

type Schema struct {
	IDs
	Changelog
}

type IDs struct {
	Version    int                      `yaml:"version"`
	MetricsIDs map[string][]VersionedID `yaml:"metrics_ids"`
}

type VersionedID struct {
	ID           MetricID `yaml:"id"`
	IntroVersion string   `yaml:"intro_version"`
}

func fetchAndUnmarshal[T any](url string, out *T) (err error) {
	var b []byte
	if strings.HasPrefix(url, "http") {
		resp, err := http.Get(url)
		if err != nil {
			return fmt.Errorf("http fetch %s: %w", url, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			// TODO(bwplotka): Print potential body?
			return fmt.Errorf("http fetch %s, got non-200 status: %d", url, resp.StatusCode)
		}

		// TODO(bwplotka): Add limit.
		b, err = io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("read all from http %s: %w", url, err)
		}
	} else {
		b, err = os.ReadFile(url)
		if err != nil {
			return fmt.Errorf("read all from file %s: %w", url, err)
		}
	}
	if err := yaml.Unmarshal(b, out); err != nil {
		return err
	}
	return nil
}
