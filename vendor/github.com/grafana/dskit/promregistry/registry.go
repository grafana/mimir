package promregistry

import "github.com/prometheus/client_golang/prometheus"

// TeeRegisterer is a slice of Registerers. It implements Registerer itself and
// forward registrations to all Registerers in the slice.
type TeeRegisterer []prometheus.Registerer

func (t TeeRegisterer) Register(c prometheus.Collector) error {
	for _, reg := range t {
		if err := reg.Register(c); err != nil {
			return err
		}
	}
	return nil
}

func (t TeeRegisterer) MustRegister(cs ...prometheus.Collector) {
	for _, reg := range t {
		reg.MustRegister(cs...)
	}
}

func (t TeeRegisterer) Unregister(c prometheus.Collector) bool {
	result := false
	for _, reg := range t {
		if reg.Unregister(c) {
			result = true
		}
	}
	return result
}
