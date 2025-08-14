/*
 * Copyright The OpenTelemetry Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package arrow

// General configuration for traces. This configuration defines the different
// sorters used for attributes, spans, events, links, ... and the encoding used
// for parent IDs.

import (
	cfg "github.com/open-telemetry/otel-arrow/go/pkg/config"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/arrow"
)

type (
	Config struct {
		Global *cfg.Config

		Span  *SpanConfig
		Event *EventConfig
		Link  *LinkConfig
		Attrs *AttrsConfig
	}

	AttrsConfig struct {
		Resource *arrow.Attrs16Config
		Scope    *arrow.Attrs16Config
		Span     *arrow.Attrs16Config
		Event    *arrow.Attrs32Config
		Link     *arrow.Attrs32Config
	}

	SpanConfig struct {
		Sorter SpanSorter
	}

	EventConfig struct {
		Sorter EventSorter
	}

	LinkConfig struct {
		Sorter LinkSorter
	}
)

func DefaultConfig() *Config {
	return NewConfig(cfg.DefaultConfig())
}

func NewConfig(globalConf *cfg.Config) *Config {
	return &Config{
		Global: globalConf,
		Span: &SpanConfig{
			Sorter: SortSpansByResourceSpanIdScopeSpanIdNameTraceId(),
		},
		Event: &EventConfig{
			Sorter: SortEventsByNameParentId(),
		},
		Link: &LinkConfig{
			Sorter: SortLinksByTraceIdParentId(),
		},
		Attrs: &AttrsConfig{
			Resource: &arrow.Attrs16Config{
				Sorter: arrow.SortAttrs16ByTypeKeyValueParentId(),
			},
			Scope: &arrow.Attrs16Config{
				Sorter: arrow.SortAttrs16ByTypeKeyValueParentId(),
			},
			Span: &arrow.Attrs16Config{
				Sorter: arrow.SortAttrs16ByTypeKeyValueParentId(),
			},
			Event: &arrow.Attrs32Config{
				Sorter: arrow.SortAttrs32ByTypeKeyValueParentId(),
			},
			Link: &arrow.Attrs32Config{
				Sorter: arrow.SortAttrs32ByTypeKeyValueParentId(),
			},
		},
	}
}

func NewNoSortConfig(globalConf *cfg.Config) *Config {
	return &Config{
		Global: globalConf,
		Span: &SpanConfig{
			Sorter: UnsortedSpans(),
		},
		Event: &EventConfig{
			Sorter: UnsortedEvents(),
		},
		Link: &LinkConfig{
			Sorter: UnsortedLinks(),
		},
		Attrs: &AttrsConfig{
			Resource: &arrow.Attrs16Config{
				Sorter: arrow.UnsortedAttrs16(),
			},
			Scope: &arrow.Attrs16Config{
				Sorter: arrow.UnsortedAttrs16(),
			},
			Span: &arrow.Attrs16Config{
				Sorter: arrow.UnsortedAttrs16(),
			},
			Event: &arrow.Attrs32Config{
				Sorter: arrow.UnsortedAttrs32(),
			},
			Link: &arrow.Attrs32Config{
				Sorter: arrow.UnsortedAttrs32(),
			},
		},
	}
}
