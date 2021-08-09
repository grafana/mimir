// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/dummy.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"net/url"

	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/scrape"
)

// DummyTargetRetriever implements github.com/prometheus/prometheus/web/api/v1.targetRetriever.
type DummyTargetRetriever struct{}

// TargetsActive implements targetRetriever.
func (DummyTargetRetriever) TargetsActive() map[string][]*scrape.Target {
	return map[string][]*scrape.Target{}
}

// TargetsDropped implements targetRetriever.
func (DummyTargetRetriever) TargetsDropped() map[string][]*scrape.Target {
	return map[string][]*scrape.Target{}
}

// DummyAlertmanagerRetriever implements AlertmanagerRetriever.
type DummyAlertmanagerRetriever struct{}

// Alertmanagers implements AlertmanagerRetriever.
func (DummyAlertmanagerRetriever) Alertmanagers() []*url.URL { return nil }

// DroppedAlertmanagers implements AlertmanagerRetriever.
func (DummyAlertmanagerRetriever) DroppedAlertmanagers() []*url.URL { return nil }

// DummyRulesRetriever implements RulesRetriever.
type DummyRulesRetriever struct{}

// RuleGroups implements RulesRetriever.
func (DummyRulesRetriever) RuleGroups() []*rules.Group {
	return nil
}

// AlertingRules implements RulesRetriever.
func (DummyRulesRetriever) AlertingRules() []*rules.AlertingRule {
	return nil
}
