package promregistry

import "github.com/prometheus/client_golang/prometheus"

// TeeRegisterer supports MultipleRegisterer.
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
