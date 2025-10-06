package alertmanager

import "github.com/prometheus/client_golang/prometheus"

// NoOpReg is no-op prometheus.Registerer.
// It's useful when we need to call functions that require a registerer but we're reusing an existing one.
type noOpReg struct{}

func (noOpReg) Register(prometheus.Collector) error  { return nil }
func (noOpReg) MustRegister(...prometheus.Collector) {}
func (noOpReg) Unregister(prometheus.Collector) bool { return false }
