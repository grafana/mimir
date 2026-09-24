// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"fmt"
	"math"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

const (
	wrappedGaussianImages = 10
	hashSpaceCardinality  = float64(uint64(math.MaxUint32) + 1)
)

// at evaluates this component's configured amplitude at one simulation tick.
func (a TemporalAmplitude) at(tick int) float64 {
	switch a.Kind {
	case "constant":
		return a.Value
	case "linear":
		if tick <= a.StartTick {
			return a.StartValue
		}
		if tick >= a.EndTick {
			return a.EndValue
		}
		fraction := float64(tick-a.StartTick) / float64(a.EndTick-a.StartTick)
		return a.StartValue + fraction*(a.EndValue-a.StartValue)
	case "gaussian":
		distance := (float64(tick) - a.CenterTick) / a.TimeWidth
		return a.Peak * math.Exp(-0.5*distance*distance)
	default:
		panic(fmt.Sprintf("unsupported temporal amplitude %q", a.Kind))
	}
}

// integrate returns the exact integral of the represented analytic workload
// over the closed uint32 hash range. Spatial Gaussians are wrapped around the
// normalized [0,1) hash ring; evaluating a fixed number of image terms is
// deterministic and leaves less than machine-scale tail mass for validated
// widths.
func (w TenantWorkload) integrate(hr assignment.HashRange, tick int) float64 {
	lo := float64(uint64(hr.Lo)) / hashSpaceCardinality
	hi := float64(uint64(hr.Hi)+1) / hashSpaceCardinality
	load := w.Baseline * (hi - lo)
	for _, component := range w.Components {
		load += component.Amplitude.at(tick) * wrappedGaussianIntegral(lo, hi, component.Center, component.Width)
	}
	return load
}

// wrappedGaussianIntegral integrates a Gaussian plus periodic images so hotspots wrap around the hash ring.
func wrappedGaussianIntegral(lo, hi, center, width float64) float64 {
	total := 0.0
	for image := -wrappedGaussianImages; image <= wrappedGaussianImages; image++ {
		imageCenter := center + float64(image)
		total += normalCDF((hi-imageCenter)/width) - normalCDF((lo-imageCenter)/width)
	}
	return total
}

// normalCDF evaluates the standard normal cumulative distribution used for analytic range integration.
func normalCDF(x float64) float64 {
	return 0.5 * (1 + math.Erf(x/math.Sqrt2))
}

// workloadChangedAt identifies the first changing-load tick used by adaptation and tracking metrics.
func (f Fixture) workloadChangedAt(tick int) bool {
	if tick <= 0 {
		return false
	}
	for _, tenant := range f.Tenants {
		for _, component := range tenant.Components {
			if math.Abs(component.Amplitude.at(tick)-component.Amplitude.at(tick-1)) > 1e-12 {
				return true
			}
		}
	}
	return false
}
