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

import (
	cfg "github.com/open-telemetry/otel-arrow/go/pkg/config"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/arrow"
)

// General configuration for metrics. This configuration defines the different
// sorters used for attributes, metrics, data points, ... and the encoding used
// for parent IDs.

type (
	Config struct {
		Global *cfg.Config

		Metric *MetricConfig

		NumberDP     *NumberDataPointConfig
		Summary      *SummaryConfig
		Histogram    *HistogramConfig
		ExpHistogram *ExpHistogramConfig

		NumberDataPointExemplar *ExemplarConfig
		HistogramExemplar       *ExemplarConfig
		ExpHistogramExemplar    *ExemplarConfig

		Attrs *AttrsConfig
	}

	AttrsConfig struct {
		Resource                *arrow.Attrs16Config
		Scope                   *arrow.Attrs16Config
		NumberDataPoint         *arrow.Attrs32Config
		NumberDataPointExemplar *arrow.Attrs32Config
		Summary                 *arrow.Attrs32Config
		Histogram               *arrow.Attrs32Config
		HistogramExemplar       *arrow.Attrs32Config
		ExpHistogram            *arrow.Attrs32Config
		ExpHistogramExemplar    *arrow.Attrs32Config
	}

	MetricConfig struct {
		Sorter MetricSorter
	}

	ExemplarConfig struct {
		Sorter ExemplarSorter
	}

	NumberDataPointConfig struct {
		Sorter NumberDataPointSorter
	}

	SummaryConfig struct {
		Sorter SummarySorter
	}

	HistogramConfig struct {
		Sorter HistogramSorter
	}

	ExpHistogramConfig struct {
		Sorter EHistogramSorter
	}
)

func DefaultConfig() *Config {
	return NewConfig(cfg.DefaultConfig())
}

func NewConfig(globalConf *cfg.Config) *Config {
	return &Config{
		Global: globalConf,
		Metric: &MetricConfig{
			Sorter: SortMetricsByResourceScopeTypeName(),
			//Sorter: SortMetricsByTypeNameResourceScope(),
		},
		NumberDP: &NumberDataPointConfig{
			//Sorter: UnsortedNumberDataPoints(), // 1.86, 1.82
			//Sorter: SortNumberDataPointsByTimeParentID(), // 2.04, 2.01
			//Sorter: SortNumberDataPointsByTimeParentIDTypeValue(), // 2.23, 2.19
			//Sorter: SortNumberDataPointsByTypeValueTimestampParentID(), // 1.75, 1.78
			//Sorter: SortNumberDataPointsByTimestampTypeValueParentID(), // 1.96, 1.91
			Sorter: SortNumberDataPointsByParentID(), // 2.31, 2.29
		},
		Summary: &SummaryConfig{
			Sorter: SortSummariesByParentID(),
		},
		Histogram: &HistogramConfig{
			Sorter: SortHistogramsByParentID(),
		},
		ExpHistogram: &ExpHistogramConfig{
			Sorter: SortEHistogramsByParentID(),
		},
		NumberDataPointExemplar: &ExemplarConfig{
			Sorter: SortExemplarsByTypeValueParentId(),
		},
		HistogramExemplar: &ExemplarConfig{
			Sorter: SortExemplarsByTypeValueParentId(),
		},
		ExpHistogramExemplar: &ExemplarConfig{
			Sorter: SortExemplarsByTypeValueParentId(),
		},
		Attrs: &AttrsConfig{
			Resource: &arrow.Attrs16Config{
				Sorter: arrow.SortAttrs16ByTypeKeyValueParentId(),
			},
			Scope: &arrow.Attrs16Config{
				Sorter: arrow.SortAttrs16ByTypeKeyValueParentId(),
			},
			NumberDataPoint: &arrow.Attrs32Config{
				Sorter: arrow.SortAttrs32ByTypeKeyValueParentId(),
			},
			NumberDataPointExemplar: &arrow.Attrs32Config{
				Sorter: arrow.SortAttrs32ByTypeKeyValueParentId(),
			},
			Summary: &arrow.Attrs32Config{
				Sorter: arrow.SortAttrs32ByTypeKeyValueParentId(),
			},
			Histogram: &arrow.Attrs32Config{
				Sorter: arrow.SortAttrs32ByTypeKeyValueParentId(),
			},
			HistogramExemplar: &arrow.Attrs32Config{
				Sorter: arrow.SortAttrs32ByTypeKeyValueParentId(),
			},
			ExpHistogram: &arrow.Attrs32Config{
				Sorter: arrow.SortAttrs32ByTypeKeyValueParentId(),
			},
			ExpHistogramExemplar: &arrow.Attrs32Config{
				Sorter: arrow.SortAttrs32ByTypeKeyValueParentId(),
			},
		},
	}
}

func NewNoSortConfig(globalConf *cfg.Config) *Config {
	return &Config{
		Global: globalConf,
		Metric: &MetricConfig{
			Sorter: UnsortedMetrics(),
		},
		NumberDP: &NumberDataPointConfig{
			Sorter: UnsortedNumberDataPoints(),
		},
		Summary: &SummaryConfig{
			Sorter: UnsortedSummaries(),
		},
		Histogram: &HistogramConfig{
			Sorter: UnsortedHistograms(),
		},
		ExpHistogram: &ExpHistogramConfig{
			Sorter: UnsortedEHistograms(),
		},
		NumberDataPointExemplar: &ExemplarConfig{
			Sorter: UnsortedExemplars(),
		},
		HistogramExemplar: &ExemplarConfig{
			Sorter: UnsortedExemplars(),
		},
		ExpHistogramExemplar: &ExemplarConfig{
			Sorter: UnsortedExemplars(),
		},
		Attrs: &AttrsConfig{
			Resource: &arrow.Attrs16Config{
				Sorter: arrow.UnsortedAttrs16(),
			},
			Scope: &arrow.Attrs16Config{
				Sorter: arrow.UnsortedAttrs16(),
			},
			NumberDataPoint: &arrow.Attrs32Config{
				Sorter: arrow.UnsortedAttrs32(),
			},
			NumberDataPointExemplar: &arrow.Attrs32Config{
				Sorter: arrow.UnsortedAttrs32(),
			},
			Summary: &arrow.Attrs32Config{
				Sorter: arrow.UnsortedAttrs32(),
			},
			Histogram: &arrow.Attrs32Config{
				Sorter: arrow.UnsortedAttrs32(),
			},
			HistogramExemplar: &arrow.Attrs32Config{
				Sorter: arrow.UnsortedAttrs32(),
			},
			ExpHistogram: &arrow.Attrs32Config{
				Sorter: arrow.UnsortedAttrs32(),
			},
			ExpHistogramExemplar: &arrow.Attrs32Config{
				Sorter: arrow.UnsortedAttrs32(),
			},
		},
	}
}
