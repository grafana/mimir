package alertmanager

import (
	"github.com/prometheus/alertmanager/api"
	"github.com/prometheus/alertmanager/config"
	"github.com/prometheus/alertmanager/notify"
	"github.com/prometheus/alertmanager/template"
	"github.com/prometheus/alertmanager/timeinterval"
	"github.com/prometheus/common/model"
)

// Configuration is an interface for accessing Alertmanager configuration.
type Configuration interface {
	InhibitRules() []config.InhibitRule
	TimeIntervals() map[string][]timeinterval.TimeInterval
	ReceiverIntegrations() map[string][]notify.Integration

	RoutingTree() *config.Route
	Templates() *template.Template

	Raw() []byte
	UpdateAPI(api *api.API)
}

type AlertmanagerConfig struct {
	conf            *config.Config
	template        *template.Template
	integrationsMap map[string][]notify.Integration

	raw string
}

func (m AlertmanagerConfig) UpdateAPI(api *api.API) {
	api.Update(m.conf, func(_ model.LabelSet) {})
}

func (m AlertmanagerConfig) TimeIntervals() map[string][]timeinterval.TimeInterval {
	timeIntervals := make(map[string][]timeinterval.TimeInterval, len(m.conf.MuteTimeIntervals)+len(m.conf.TimeIntervals))
	for _, ti := range m.conf.MuteTimeIntervals {
		timeIntervals[ti.Name] = ti.TimeIntervals
	}

	for _, ti := range m.conf.TimeIntervals {
		timeIntervals[ti.Name] = ti.TimeIntervals
	}
	return timeIntervals
}

func (m AlertmanagerConfig) InhibitRules() []config.InhibitRule {
	return m.conf.InhibitRules
}

func (m AlertmanagerConfig) ReceiverIntegrations() map[string][]notify.Integration {
	return m.integrationsMap
}

func (m AlertmanagerConfig) RoutingTree() *config.Route {
	return m.conf.Route
}

func (m AlertmanagerConfig) Raw() []byte {
	return []byte(m.raw)
}

func (m AlertmanagerConfig) Templates() *template.Template {
	return m.template
}
