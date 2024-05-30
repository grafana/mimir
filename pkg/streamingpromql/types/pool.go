// SPDX-License-Identifier: AGPL-3.0-only

package types

import "github.com/prometheus/prometheus/promql"

type SampleSlicePool interface {
	GetFPointSlice(size int) ([]promql.FPoint, error)
	PutFPointSlice(s []promql.FPoint)

	GetHPointSlice(size int) ([]promql.HPoint, error)
	PutHPointSlice(s []promql.HPoint)
}
