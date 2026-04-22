package prom

import (
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

// MustRegisterOrGet registers a Collector or returns a previously-registered Collector with the same description.
// Like prometheus.MustRegister, but only panics if registration error is not prometheus.AlreadyRegisteredError.
//
// Intended for types which perform metric Collector registration in their constructors,
// but may be constructed multiple times with equivalent Registry & Collectors.
func MustRegisterOrGet[T prometheus.Collector](reg prometheus.Registerer, c T) T {
	if err := reg.Register(c); err != nil {
		var are prometheus.AlreadyRegisteredError
		if errors.As(err, &are) {
			return are.ExistingCollector.(T)
		}
		panic(err)
	}
	return c
}
