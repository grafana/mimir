package definition

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/prometheus/alertmanager/config"
	"github.com/prometheus/alertmanager/pkg/labels"
	"github.com/prometheus/common/model"
)

type Provenance string

// Config is the top-level configuration for Alertmanager's config files.
type Config struct {
	Global       *config.GlobalConfig `yaml:"global,omitempty" json:"global,omitempty"`
	Route        *Route               `yaml:"route,omitempty" json:"route,omitempty"`
	InhibitRules []config.InhibitRule `yaml:"inhibit_rules,omitempty" json:"inhibit_rules,omitempty"`
	// MuteTimeIntervals is deprecated and will be removed before Alertmanager 1.0.
	MuteTimeIntervals []config.MuteTimeInterval `yaml:"mute_time_intervals,omitempty" json:"mute_time_intervals,omitempty"`
	TimeIntervals     []config.TimeInterval     `yaml:"time_intervals,omitempty" json:"time_intervals,omitempty"`
	Templates         []string                  `yaml:"templates" json:"templates"`
}

// A Route is a node that contains definitions of how to handle alerts. This is modified
// from the upstream alertmanager in that it adds the ObjectMatchers property.
type Route struct {
	Receiver string `yaml:"receiver,omitempty" json:"receiver,omitempty"`

	GroupByStr []string          `yaml:"group_by,omitempty" json:"group_by,omitempty"`
	GroupBy    []model.LabelName `yaml:"-" json:"-"`
	GroupByAll bool              `yaml:"-" json:"-"`
	// Deprecated. Remove before v1.0 release.
	Match map[string]string `yaml:"match,omitempty" json:"match,omitempty"`
	// Deprecated. Remove before v1.0 release.
	MatchRE           config.MatchRegexps `yaml:"match_re,omitempty" json:"match_re,omitempty"`
	Matchers          config.Matchers     `yaml:"matchers,omitempty" json:"matchers,omitempty"`
	ObjectMatchers    ObjectMatchers      `yaml:"object_matchers,omitempty" json:"object_matchers,omitempty"`
	MuteTimeIntervals []string            `yaml:"mute_time_intervals,omitempty" json:"mute_time_intervals,omitempty"`
	Continue          bool                `yaml:"continue" json:"continue,omitempty"`
	Routes            []*Route            `yaml:"routes,omitempty" json:"routes,omitempty"`

	GroupWait      *model.Duration `yaml:"group_wait,omitempty" json:"group_wait,omitempty"`
	GroupInterval  *model.Duration `yaml:"group_interval,omitempty" json:"group_interval,omitempty"`
	RepeatInterval *model.Duration `yaml:"repeat_interval,omitempty" json:"repeat_interval,omitempty"`

	Provenance Provenance `yaml:"provenance,omitempty" json:"provenance,omitempty"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface for Route. This is a copy of alertmanager's upstream except it removes validation on the label key.
func (r *Route) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type plain Route
	if err := unmarshal((*plain)(r)); err != nil {
		return err
	}

	return r.ValidateChild()
}

// AsAMRoute returns an Alertmanager route from a Grafana route. The ObjectMatchers are converted to Matchers.
func (r *Route) AsAMRoute() *config.Route {
	amRoute := &config.Route{
		Receiver:          r.Receiver,
		GroupByStr:        r.GroupByStr,
		GroupBy:           r.GroupBy,
		GroupByAll:        r.GroupByAll,
		Match:             r.Match,
		MatchRE:           r.MatchRE,
		Matchers:          append(r.Matchers, r.ObjectMatchers...),
		MuteTimeIntervals: r.MuteTimeIntervals,
		Continue:          r.Continue,

		GroupWait:      r.GroupWait,
		GroupInterval:  r.GroupInterval,
		RepeatInterval: r.RepeatInterval,

		Routes: make([]*config.Route, 0, len(r.Routes)),
	}
	for _, rt := range r.Routes {
		amRoute.Routes = append(amRoute.Routes, rt.AsAMRoute())
	}

	return amRoute
}

// AsGrafanaRoute returns a Grafana route from an Alertmanager route.
func AsGrafanaRoute(r *config.Route) *Route {
	gRoute := &Route{
		Receiver:          r.Receiver,
		GroupByStr:        r.GroupByStr,
		GroupBy:           r.GroupBy,
		GroupByAll:        r.GroupByAll,
		Match:             r.Match,
		MatchRE:           r.MatchRE,
		Matchers:          r.Matchers,
		MuteTimeIntervals: r.MuteTimeIntervals,
		Continue:          r.Continue,

		GroupWait:      r.GroupWait,
		GroupInterval:  r.GroupInterval,
		RepeatInterval: r.RepeatInterval,

		Routes: make([]*Route, 0, len(r.Routes)),
	}
	for _, rt := range r.Routes {
		gRoute.Routes = append(gRoute.Routes, AsGrafanaRoute(rt))
	}

	return gRoute
}

func (r *Route) ResourceType() string {
	return "route"
}

func (r *Route) ResourceID() string {
	return ""
}

// Config is the entrypoint for the embedded Alertmanager config with the exception of receivers.
// Prometheus historically uses yaml files as the method of configuration and thus some
// post-validation is included in the UnmarshalYAML method. Here we simply run this with
// a noop unmarshaling function in order to benefit from said validation.
func (c *Config) UnmarshalJSON(b []byte) error {
	type plain Config
	if err := json.Unmarshal(b, (*plain)(c)); err != nil {
		return err
	}

	noopUnmarshal := func(_ interface{}) error { return nil }

	if c.Global != nil {
		if err := c.Global.UnmarshalYAML(noopUnmarshal); err != nil {
			return err
		}
	}

	if c.Route == nil {
		return fmt.Errorf("no routes provided")
	}

	err := c.Route.Validate()
	if err != nil {
		return err
	}

	for _, r := range c.InhibitRules {
		if err := r.UnmarshalYAML(noopUnmarshal); err != nil {
			return err
		}
	}

	tiNames := make(map[string]struct{})
	for _, mt := range c.MuteTimeIntervals {
		if mt.Name == "" {
			return fmt.Errorf("missing name in mute time interval")
		}
		if _, ok := tiNames[mt.Name]; ok {
			return fmt.Errorf("mute time interval %q is not unique", mt.Name)
		}
		tiNames[mt.Name] = struct{}{}
	}
	for _, ti := range c.TimeIntervals {
		if ti.Name == "" {
			return fmt.Errorf("missing name in time interval")
		}
		if _, ok := tiNames[ti.Name]; ok {
			return fmt.Errorf("time interval %q is not unique", ti.Name)
		}
		tiNames[ti.Name] = struct{}{}
	}
	return checkTimeInterval(c.Route, tiNames)
}

func checkTimeInterval(r *Route, timeIntervals map[string]struct{}) error {
	for _, sr := range r.Routes {
		if err := checkTimeInterval(sr, timeIntervals); err != nil {
			return err
		}
	}
	if len(r.MuteTimeIntervals) == 0 {
		return nil
	}
	for _, mt := range r.MuteTimeIntervals {
		if _, ok := timeIntervals[mt]; !ok {
			return fmt.Errorf("undefined time interval %q used in route", mt)
		}
	}
	return nil
}

// nolint:revive
type PostableApiAlertingConfig struct {
	Config `yaml:",inline"`

	// Override with our superset receiver type
	Receivers []*PostableApiReceiver `yaml:"receivers,omitempty" json:"receivers,omitempty"`
}

func (c *PostableApiAlertingConfig) GetReceivers() []*PostableApiReceiver {
	return c.Receivers
}

func (c *PostableApiAlertingConfig) GetMuteTimeIntervals() []config.MuteTimeInterval {
	return c.MuteTimeIntervals
}

func (c *PostableApiAlertingConfig) GetTimeIntervals() []config.TimeInterval { return c.TimeIntervals }

func (c *PostableApiAlertingConfig) GetRoute() *Route {
	return c.Route
}

func (c *PostableApiAlertingConfig) UnmarshalJSON(b []byte) error {
	type plain PostableApiAlertingConfig
	if err := json.Unmarshal(b, (*plain)(c)); err != nil {
		return err
	}

	// Since Config implements json.Unmarshaler, we must handle _all_ other fields independently.
	// Otherwise, the json decoder will detect this and only use the embedded type.
	// Additionally, we'll use pointers to slices in order to reference the intended target.
	type overrides struct {
		Receivers *[]*PostableApiReceiver `yaml:"receivers,omitempty" json:"receivers,omitempty"`
	}

	if err := json.Unmarshal(b, &overrides{Receivers: &c.Receivers}); err != nil {
		return err
	}

	return c.Validate()
}

// Validate ensures that the two routing trees use the correct receiver types.
func (c *PostableApiAlertingConfig) Validate() error {
	receivers := make(map[string]struct{}, len(c.Receivers))

	var hasGrafReceivers, hasAMReceivers bool
	for _, r := range c.Receivers {
		receivers[r.Name] = struct{}{}
		switch r.Type() {
		case GrafanaReceiverType:
			hasGrafReceivers = true
		case AlertmanagerReceiverType:
			hasAMReceivers = true
		default:
			continue
		}
	}

	if hasGrafReceivers && hasAMReceivers {
		return fmt.Errorf("cannot mix Alertmanager & Grafana receiver types")
	}

	// Taken from https://github.com/prometheus/alertmanager/blob/14cbe6301c732658d6fe877ec55ad5b738abcf06/config/config.go#L171-L192
	// Check if we have a root route. We cannot check for it in the
	// UnmarshalYAML method because it won't be called if the input is empty
	// (e.g. the config file is empty or only contains whitespace).
	if c.Route == nil {
		return fmt.Errorf("no route provided in config")
	}

	// Check if continue in root route.
	if c.Route.Continue {
		return fmt.Errorf("cannot have continue in root route")
	}

	for _, receiver := range AllReceivers(c.Route.AsAMRoute()) {
		_, ok := receivers[receiver]
		if !ok {
			return fmt.Errorf("unexpected receiver (%s) is undefined", receiver)
		}
	}

	return nil
}

// Type requires validate has been called and just checks the first receiver type
func (c *PostableApiAlertingConfig) ReceiverType() ReceiverType {
	for _, r := range c.Receivers {
		switch r.Type() {
		case GrafanaReceiverType:
			return GrafanaReceiverType
		case AlertmanagerReceiverType:
			return AlertmanagerReceiverType
		default:
			continue
		}
	}
	return EmptyReceiverType
}

// AllReceivers will recursively walk a routing tree and return a list of all the
// referenced receiver names.
func AllReceivers(route *config.Route) (res []string) {
	if route == nil {
		return res
	}
	// TODO: Consider removing this check when new resource-specific AM APIs are implemented.
	// Skip autogenerated routes. This helps cover the case where an admin POSTs the autogenerated route back to us.
	// For example, when deleting a contact point that is unused but still referenced in the autogenerated route.
	if isAutogeneratedRoot(route) {
		return nil
	}

	if route.Receiver != "" {
		res = append(res, route.Receiver)
	}

	for _, subRoute := range route.Routes {
		res = append(res, AllReceivers(subRoute)...)
	}
	return res
}

// autogeneratedRouteLabel a label name used to distinguish alerts that are supposed to be handled by the autogenerated policy. Only expected value is `true`.
const autogeneratedRouteLabel = "__grafana_autogenerated__"

// isAutogeneratedRoot returns true if the route is the root of an autogenerated route.
func isAutogeneratedRoot(route *config.Route) bool {
	return len(route.Matchers) == 1 && route.Matchers[0].Name == autogeneratedRouteLabel
}

type RawMessage json.RawMessage // This type alias adds YAML marshaling to the json.RawMessage.

// MarshalJSON returns m as the JSON encoding of m.
func (r RawMessage) MarshalJSON() ([]byte, error) {
	return json.Marshal(json.RawMessage(r))
}

func (r *RawMessage) UnmarshalJSON(data []byte) error {
	var raw json.RawMessage
	err := json.Unmarshal(data, &raw)
	if err != nil {
		return err
	}
	*r = RawMessage(raw)
	return nil
}

func (r *RawMessage) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var data interface{}
	if err := unmarshal(&data); err != nil {
		return err
	}
	bytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	*r = bytes
	return nil
}

func (r RawMessage) MarshalYAML() (interface{}, error) {
	if r == nil {
		return nil, nil
	}
	var d interface{}
	err := json.Unmarshal(r, &d)
	if err != nil {
		return nil, err
	}
	return d, nil
}

type PostableGrafanaReceiver struct {
	UID                   string            `json:"uid"`
	Name                  string            `json:"name"`
	Type                  string            `json:"type"`
	DisableResolveMessage bool              `json:"disableResolveMessage"`
	Settings              RawMessage        `json:"settings,omitempty"`
	SecureSettings        map[string]string `json:"secureSettings"`
}

type ReceiverType int

const (
	GrafanaReceiverType ReceiverType = 1 << iota
	AlertmanagerReceiverType
	EmptyReceiverType = GrafanaReceiverType | AlertmanagerReceiverType
)

func (r ReceiverType) String() string {
	switch r {
	case GrafanaReceiverType:
		return "grafana"
	case AlertmanagerReceiverType:
		return "alertmanager"
	case EmptyReceiverType:
		return "empty"
	default:
		return "unknown"
	}
}

// Can determines whether a receiver type can implement another receiver type.
// This is useful as receivers with just names but no contact points
// are valid in all backends.
func (r ReceiverType) Can(other ReceiverType) bool { return r&other != 0 }

// ObjectMatcher is a matcher that can be used to filter alerts.
// swagger:model ObjectMatcher
type ObjectMatcherAPIModel [3]string

// ObjectMatchers is a list of matchers that can be used to filter alerts.
// swagger:model ObjectMatchers
type ObjectMatchersAPIModel []ObjectMatcherAPIModel

// swagger:ignore
// ObjectMatchers is Matchers with a different Unmarshal and Marshal methods that accept matchers as objects
// that have already been parsed.
type ObjectMatchers labels.Matchers

// UnmarshalYAML implements the yaml.Unmarshaler interface for Matchers.
func (m *ObjectMatchers) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var rawMatchers ObjectMatchersAPIModel
	if err := unmarshal(&rawMatchers); err != nil {
		return err
	}
	for _, rawMatcher := range rawMatchers {
		var matchType labels.MatchType
		switch rawMatcher[1] {
		case "=":
			matchType = labels.MatchEqual
		case "!=":
			matchType = labels.MatchNotEqual
		case "=~":
			matchType = labels.MatchRegexp
		case "!~":
			matchType = labels.MatchNotRegexp
		default:
			return fmt.Errorf("unsupported match type %q in matcher", rawMatcher[1])
		}

		// When Prometheus serializes a matcher, the value gets wrapped in quotes:
		// https://github.com/prometheus/alertmanager/blob/main/pkg/labels/matcher.go#L77
		// Remove these quotes so that we are matching against the right value.
		//
		// This is a stop-gap solution which will be superceded by https://github.com/grafana/grafana/issues/50040.
		//
		// The ngalert migration converts matchers into the Prom-style, quotes included.
		// The UI then stores the quotes into ObjectMatchers without removing them.
		// This approach allows these extra quotes to be stored in the database, and fixes them at read time.
		// This works because the database stores matchers as JSON text.
		//
		// There is a subtle bug here, where users might intentionally add quotes to matchers. This method can remove such quotes.
		// Since ObjectMatchers will be deprecated entirely, this bug will go away naturally with time.
		rawMatcher[2] = strings.TrimPrefix(rawMatcher[2], "\"")
		rawMatcher[2] = strings.TrimSuffix(rawMatcher[2], "\"")

		matcher, err := labels.NewMatcher(matchType, rawMatcher[0], rawMatcher[2])
		if err != nil {
			return err
		}
		*m = append(*m, matcher)
	}
	sort.Sort(labels.Matchers(*m))
	return nil
}

// UnmarshalJSON implements the json.Unmarshaler interface for Matchers.
func (m *ObjectMatchers) UnmarshalJSON(data []byte) error {
	var rawMatchers ObjectMatchersAPIModel
	if err := json.Unmarshal(data, &rawMatchers); err != nil {
		return err
	}
	for _, rawMatcher := range rawMatchers {
		var matchType labels.MatchType
		switch rawMatcher[1] {
		case "=":
			matchType = labels.MatchEqual
		case "!=":
			matchType = labels.MatchNotEqual
		case "=~":
			matchType = labels.MatchRegexp
		case "!~":
			matchType = labels.MatchNotRegexp
		default:
			return fmt.Errorf("unsupported match type %q in matcher", rawMatcher[1])
		}

		rawMatcher[2] = strings.TrimPrefix(rawMatcher[2], "\"")
		rawMatcher[2] = strings.TrimSuffix(rawMatcher[2], "\"")

		matcher, err := labels.NewMatcher(matchType, rawMatcher[0], rawMatcher[2])
		if err != nil {
			return err
		}
		*m = append(*m, matcher)
	}
	sort.Sort(labels.Matchers(*m))
	return nil
}

// MarshalYAML implements the yaml.Marshaler interface for Matchers.
func (m ObjectMatchers) MarshalYAML() (interface{}, error) {
	result := make(ObjectMatchersAPIModel, len(m))
	for i, matcher := range m {
		result[i] = ObjectMatcherAPIModel{matcher.Name, matcher.Type.String(), matcher.Value}
	}
	return result, nil
}

// MarshalJSON implements the json.Marshaler interface for Matchers.
func (m ObjectMatchers) MarshalJSON() ([]byte, error) {
	if len(m) == 0 {
		return nil, nil
	}
	result := make(ObjectMatchersAPIModel, len(m))
	for i, matcher := range m {
		result[i] = ObjectMatcherAPIModel{matcher.Name, matcher.Type.String(), matcher.Value}
	}
	return json.Marshal(result)
}

// nolint:revive
type PostableApiReceiver struct {
	config.Receiver          `yaml:",inline"`
	PostableGrafanaReceivers `yaml:",inline"`
}

func (r *PostableApiReceiver) UnmarshalYAML(unmarshal func(interface{}) error) error {
	if err := unmarshal(&r.PostableGrafanaReceivers); err != nil {
		return err
	}

	if err := unmarshal(&r.Receiver); err != nil {
		return err
	}

	return nil
}

func (r *PostableApiReceiver) UnmarshalJSON(b []byte) error {
	type plain PostableApiReceiver
	if err := json.Unmarshal(b, (*plain)(r)); err != nil {
		return err
	}

	hasGrafanaReceivers := len(r.PostableGrafanaReceivers.GrafanaManagedReceivers) > 0

	if hasGrafanaReceivers {
		if len(r.EmailConfigs) > 0 {
			return fmt.Errorf("cannot have both Alertmanager EmailConfigs & Grafana receivers together")
		}
		if len(r.PagerdutyConfigs) > 0 {
			return fmt.Errorf("cannot have both Alertmanager PagerdutyConfigs & Grafana receivers together")
		}
		if len(r.SlackConfigs) > 0 {
			return fmt.Errorf("cannot have both Alertmanager SlackConfigs & Grafana receivers together")
		}
		if len(r.WebhookConfigs) > 0 {
			return fmt.Errorf("cannot have both Alertmanager WebhookConfigs & Grafana receivers together")
		}
		if len(r.OpsGenieConfigs) > 0 {
			return fmt.Errorf("cannot have both Alertmanager OpsGenieConfigs & Grafana receivers together")
		}
		if len(r.WechatConfigs) > 0 {
			return fmt.Errorf("cannot have both Alertmanager WechatConfigs & Grafana receivers together")
		}
		if len(r.PushoverConfigs) > 0 {
			return fmt.Errorf("cannot have both Alertmanager PushoverConfigs & Grafana receivers together")
		}
		if len(r.VictorOpsConfigs) > 0 {
			return fmt.Errorf("cannot have both Alertmanager VictorOpsConfigs & Grafana receivers together")
		}
	}
	return nil
}

func (r *PostableApiReceiver) Type() ReceiverType {
	if len(r.PostableGrafanaReceivers.GrafanaManagedReceivers) > 0 {
		return GrafanaReceiverType
	}

	cpy := r.Receiver
	cpy.Name = ""
	if reflect.ValueOf(cpy).IsZero() {
		return EmptyReceiverType
	}

	return AlertmanagerReceiverType
}

func (r *PostableApiReceiver) GetName() string {
	return r.Receiver.Name
}

type PostableGrafanaReceivers struct {
	GrafanaManagedReceivers []*PostableGrafanaReceiver `yaml:"grafana_managed_receiver_configs,omitempty" json:"grafana_managed_receiver_configs,omitempty"`
}
