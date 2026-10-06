// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
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

// prepareFixtureTrafficNoise precomputes placement-independent amplitude multipliers for every tick.
//
// Each tenant Gaussian component receives its own deterministic random stream,
// seeded from the fixture seed, tenant ID, and component index. That stream
// drives a stationary AR(1) process in log space:
//
//	z[0] = e[0]
//	z[t] = rho*z[t-1] + sqrt(1-rho²)*e[t]
//
// where e[t] is standard normal noise and rho controls tick-to-tick
// correlation. Each z[t] is transformed into a mean-preserving log-normal
// multiplier with the requested coefficient of variation. The multiplier is
// applied to the component amplitude before spatial integration, so noise is
// independent of the current hash-range layout and split/merge operations
// conserve load exactly. Precomputing the full trajectory makes repeated
// observations at the same tick identical.
func prepareFixtureTrafficNoise(fixture Fixture) Fixture {
	noise := fixture.effectiveTrafficNoise()
	fixture.TrafficNoise = &noise
	fixture.Tenants = append([]TenantWorkload(nil), fixture.Tenants...)
	for tenantIndex := range fixture.Tenants {
		tenant := &fixture.Tenants[tenantIndex]
		tenant.Components = append([]GaussianComponent(nil), tenant.Components...)
		for componentIndex := range tenant.Components {
			tenant.Components[componentIndex].noiseMultipliers = nil
		}
	}
	if noise.CoefficientOfVariation == 0 {
		return fixture
	}

	logDeviation := math.Sqrt(math.Log1p(noise.CoefficientOfVariation * noise.CoefficientOfVariation))
	rho := math.Exp(-1 / noise.CorrelationTicks)
	shockScale := math.Sqrt(1 - rho*rho)
	for tenantIndex := range fixture.Tenants {
		tenant := &fixture.Tenants[tenantIndex]
		for componentIndex := range tenant.Components {
			component := &tenant.Components[componentIndex]
			normal := newDeterministicNormal(noise.Seed, tenant.ID, componentIndex)
			component.noiseMultipliers = make([]float64, fixture.Ticks)
			state := normal()
			for tick := range component.noiseMultipliers {
				if tick > 0 {
					state = rho*state + shockScale*normal()
				}
				component.noiseMultipliers[tick] =
					math.Exp(logDeviation*state - 0.5*logDeviation*logDeviation)
			}
		}
	}
	return fixture
}

// newDeterministicNormal returns a stable standard-normal stream for one fixture component.
func newDeterministicNormal(seed int64, tenantID string, componentIndex int) func() float64 {
	hash := fnv.New64a()
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], uint64(seed))
	_, _ = hash.Write(encoded[:])
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(tenantID))
	_, _ = hash.Write([]byte{0})
	binary.LittleEndian.PutUint64(encoded[:], uint64(componentIndex))
	_, _ = hash.Write(encoded[:])
	state := hash.Sum64()
	return func() float64 {
		u1 := splitMixUniform(&state)
		u2 := splitMixUniform(&state)
		return math.Sqrt(-2*math.Log(u1)) * math.Cos(2*math.Pi*u2)
	}
}

// splitMixUniform advances one deterministic stream and returns a value strictly inside (0,1).
func splitMixUniform(state *uint64) float64 {
	*state += 0x9e3779b97f4a7c15
	value := *state
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	value ^= value >> 31
	return (float64(value>>11) + 0.5) / (1 << 53)
}

// at returns this component's configured amplitude after deterministic traffic variation.
func (c GaussianComponent) at(tick int) float64 {
	multiplier := 1.0
	if tick >= 0 && tick < len(c.noiseMultipliers) {
		multiplier = c.noiseMultipliers[tick]
	}
	return c.Amplitude.at(tick) * multiplier
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
		load += component.at(tick) * wrappedGaussianIntegral(lo, hi, component.Center, component.Width)
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
			if math.Abs(component.at(tick)-component.at(tick-1)) > 1e-12 {
				return true
			}
		}
	}
	return false
}
