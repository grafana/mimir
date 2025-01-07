package notify

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	tmplhtml "html/template"
	"net/http"
	"sort"
	"strings"
	"sync"
	tmpltext "text/template"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/go-openapi/strfmt"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/alerting/cluster"
	"github.com/grafana/alerting/notify/nfstatus"
	amv2 "github.com/prometheus/alertmanager/api/v2/models"
	"github.com/prometheus/alertmanager/config"
	"github.com/prometheus/alertmanager/dispatch"
	"github.com/prometheus/alertmanager/featurecontrol"
	"github.com/prometheus/alertmanager/inhibit"
	"github.com/prometheus/alertmanager/matchers/compat"
	"github.com/prometheus/alertmanager/nflog"
	"github.com/prometheus/alertmanager/nflog/nflogpb"
	"github.com/prometheus/alertmanager/notify"
	"github.com/prometheus/alertmanager/provider/mem"
	"github.com/prometheus/alertmanager/silence"
	"github.com/prometheus/alertmanager/template"
	"github.com/prometheus/alertmanager/timeinterval"
	"github.com/prometheus/alertmanager/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"

	"github.com/grafana/alerting/models"
	"github.com/grafana/alerting/templates"
)

const (
	// defaultResolveTimeout is the default timeout used for resolving an alert
	// if the end time is not specified.
	defaultResolveTimeout = 5 * time.Minute
	// memoryAlertsGCInterval is the interval at which we'll remove resolved alerts from memory.
	memoryAlertsGCInterval = 30 * time.Minute
	// snapshotPlaceholder is not a real snapshot file and will not be used, a non-empty string is required to run the maintenance function on shutdown.
	// See https://github.com/prometheus/alertmanager/blob/3ee2cd0f1271e277295c02b6160507b4d193dde2/silence/silence.go#L435-L438
	snapshotPlaceholder = "snapshot"
)

func init() {
	// This initializes the compat package in fallback mode. It parses first using the UTF-8 parser
	// and then fallsback to the classic parser on error. UTF-8 is permitted in label names.
	// This should be removed when the compat package is removed from Alertmanager.
	compat.InitFromFlags(log.NewNopLogger(), featurecontrol.NoopFlags{})
}

type ClusterPeer interface {
	AddState(string, cluster.State, prometheus.Registerer) cluster.ClusterChannel
	Position() int
	WaitReady(context.Context) error
}

type GrafanaAlertmanager struct {
	logger  log.Logger
	Metrics *GrafanaAlertmanagerMetrics

	tenantID int64

	marker      types.Marker
	alerts      *mem.Alerts
	route       *dispatch.Route
	peer        ClusterPeer
	peerTimeout time.Duration

	// wg is for dispatcher, inhibitor, silences and notifications
	// Across configuration changes dispatcher and inhibitor are completely replaced, however, silences, notification log and alerts remain the same.
	// stopc is used to let silences and notifications know we are done.
	wg    sync.WaitGroup
	stopc chan struct{}

	notificationLog *nflog.Log
	dispatcher      *dispatch.Dispatcher
	inhibitor       *inhibit.Inhibitor
	silencer        *silence.Silencer
	silences        *silence.Silences

	// timeIntervals is the set of all time_intervals and mute_time_intervals from
	// the configuration.
	timeIntervals map[string][]timeinterval.TimeInterval

	stageMetrics      *notify.Metrics
	dispatcherMetrics *dispatch.DispatcherMetrics

	reloadConfigMtx sync.RWMutex
	configHash      [16]byte
	config          []byte
	receivers       []*nfstatus.Receiver

	// buildReceiverIntegrationsFunc builds the integrations for a receiver based on its APIReceiver configuration and the current parsed template.
	buildReceiverIntegrationsFunc func(next *APIReceiver, tmpl *templates.Template) ([]*Integration, error)
	externalURL                   string

	// templates contains the template name -> template contents for each user-defined template.
	templates []templates.TemplateDefinition
}

// State represents any of the two 'states' of the alertmanager. Notification log or Silences.
// MarshalBinary returns the binary representation of this internal state based on the protobuf.
type State interface {
	MarshalBinary() ([]byte, error)
}

// MaintenanceOptions represent the configuration options available for executing maintenance of Silences and the Notification log that the Alertmanager uses.
type MaintenanceOptions interface {
	// InitialState returns the initial snapshot of the artefacts under maintenance. This will be loaded when the Alertmanager starts.
	InitialState() string
	// Retention represents for how long should we keep the artefacts under maintenance.
	Retention() time.Duration
	// MaintenanceFrequency represents how often should we execute the maintenance.
	MaintenanceFrequency() time.Duration
	// MaintenanceFunc returns the function to execute as part of the maintenance process. This will usually take a snaphot of the artefacts under maintenance.
	// It returns the size of the state in bytes or an error if the maintenance fails.
	MaintenanceFunc(state State) (int64, error)
}

var NewIntegration = nfstatus.NewIntegration

type InhibitRule = config.InhibitRule
type MuteTimeInterval = config.MuteTimeInterval
type TimeInterval = config.TimeInterval
type Route = config.Route
type Integration = nfstatus.Integration
type DispatcherLimits = dispatch.Limits
type Notifier = notify.Notifier

//nolint:revive
type NotifyReceiver = nfstatus.Receiver

// Configuration is an interface for accessing Alertmanager configuration.
type Configuration interface {
	DispatcherLimits() DispatcherLimits
	InhibitRules() []InhibitRule
	TimeIntervals() []TimeInterval
	// Deprecated: MuteTimeIntervals are deprecated in Alertmanager and will be removed in future versions.
	MuteTimeIntervals() []MuteTimeInterval
	Receivers() []*APIReceiver
	BuildReceiverIntegrationsFunc() func(next *APIReceiver, tmpl *templates.Template) ([]*Integration, error)

	RoutingTree() *Route
	Templates() []templates.TemplateDefinition

	Hash() [16]byte
	Raw() []byte
}

type Limits struct {
	MaxSilences         int
	MaxSilenceSizeBytes int
}

type GrafanaAlertmanagerConfig struct {
	ExternalURL        string
	AlertStoreCallback mem.AlertStoreCallback
	PeerTimeout        time.Duration

	Silences MaintenanceOptions
	Nflog    MaintenanceOptions

	Limits Limits
}

func (c *GrafanaAlertmanagerConfig) Validate() error {
	if c.Silences == nil {
		return errors.New("silence maintenance options must be present")
	}

	if c.Nflog == nil {
		return errors.New("notification log maintenance options must be present")
	}

	return nil
}

// NewGrafanaAlertmanager creates a new Grafana-specific Alertmanager.
func NewGrafanaAlertmanager(tenantKey string, tenantID int64, config *GrafanaAlertmanagerConfig, peer ClusterPeer, logger log.Logger, m *GrafanaAlertmanagerMetrics) (*GrafanaAlertmanager, error) {
	// TODO: Remove the context.
	am := &GrafanaAlertmanager{
		stopc:             make(chan struct{}),
		logger:            log.With(logger, "component", "alertmanager", tenantKey, tenantID),
		marker:            types.NewMarker(m.Registerer),
		stageMetrics:      notify.NewMetrics(m.Registerer, featurecontrol.NoopFlags{}),
		dispatcherMetrics: dispatch.NewDispatcherMetrics(false, m.Registerer),
		peer:              peer,
		peerTimeout:       config.PeerTimeout,
		Metrics:           m,
		tenantID:          tenantID,
		externalURL:       config.ExternalURL,
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}

	var err error

	// Initialize silences
	am.silences, err = silence.New(silence.Options{
		Metrics:        m.Registerer,
		SnapshotReader: strings.NewReader(config.Silences.InitialState()),
		Retention:      config.Silences.Retention(),
		Limits: silence.Limits{
			MaxSilences:         func() int { return config.Limits.MaxSilences },
			MaxSilenceSizeBytes: func() int { return config.Limits.MaxSilenceSizeBytes },
		},
	})
	if err != nil {
		return nil, fmt.Errorf("unable to initialize the silencing component of alerting: %w", err)
	}

	// Initialize the notification log
	am.notificationLog, err = nflog.New(nflog.Options{
		SnapshotReader: strings.NewReader(config.Nflog.InitialState()),
		Retention:      config.Nflog.Retention(),
		Logger:         logger,
		Metrics:        m.Registerer,
	})

	if err != nil {
		return nil, fmt.Errorf("unable to initialize the notification log component of alerting: %w", err)
	}
	c := am.peer.AddState(fmt.Sprintf("notificationlog:%d", am.tenantID), am.notificationLog, m.Registerer)
	am.notificationLog.SetBroadcast(c.Broadcast)

	c = am.peer.AddState(fmt.Sprintf("silences:%d", am.tenantID), am.silences, m.Registerer)
	am.silences.SetBroadcast(c.Broadcast)

	am.wg.Add(1)
	go func() {
		am.notificationLog.Maintenance(config.Nflog.MaintenanceFrequency(), snapshotPlaceholder, am.stopc, func() (int64, error) {
			if _, err := am.notificationLog.GC(); err != nil {
				level.Error(am.logger).Log("notification log garbage collection", "err", err)
			}

			return config.Nflog.MaintenanceFunc(am.notificationLog)
		})
		am.wg.Done()
	}()

	am.wg.Add(1)
	go func() {
		am.silences.Maintenance(config.Silences.MaintenanceFrequency(), snapshotPlaceholder, am.stopc, func() (int64, error) {
			// Delete silences older than the retention period.
			if _, err := am.silences.GC(); err != nil {
				level.Error(am.logger).Log("silence garbage collection", "err", err)
				// Don't return here - we need to snapshot our state first.
			}

			// Snapshot our silences to the Grafana KV store
			return config.Silences.MaintenanceFunc(am.silences)
		})
		am.wg.Done()
	}()

	// Initialize in-memory alerts
	am.alerts, err = mem.NewAlerts(context.Background(), am.marker, memoryAlertsGCInterval, config.AlertStoreCallback, am.logger, m.Registerer)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize the alert provider component of alerting: %w", err)
	}

	return am, nil
}

func (am *GrafanaAlertmanager) Ready() bool {
	// We consider AM as ready only when the config has been
	// applied at least once successfully. Until then, some objects
	// can still be nil.
	am.reloadConfigMtx.RLock()
	defer am.reloadConfigMtx.RUnlock()

	return am.ready()
}

func (am *GrafanaAlertmanager) ready() bool {
	return am.config != nil
}

func (am *GrafanaAlertmanager) StopAndWait() {
	if am.dispatcher != nil {
		am.dispatcher.Stop()
	}

	if am.inhibitor != nil {
		am.inhibitor.Stop()
	}

	am.alerts.Close()

	close(am.stopc)

	am.wg.Wait()
}

// GetReceivers returns the receivers configured as part of the current configuration.
// It is safe to call concurrently.
func (am *GrafanaAlertmanager) GetReceivers() []models.Receiver {
	am.reloadConfigMtx.RLock()
	receivers := am.receivers
	am.reloadConfigMtx.RUnlock()

	return GetReceivers(receivers)
}

// GetReceivers converts the internal receiver status into the API response.
func GetReceivers(receivers []*nfstatus.Receiver) []models.Receiver {
	apiReceivers := make([]models.Receiver, 0, len(receivers))
	for _, rcv := range receivers {
		// Build integrations slice for each receiver.
		integrations := make([]models.Integration, 0, len(rcv.Integrations()))
		for _, integration := range rcv.Integrations() {
			ts, d, err := integration.GetReport()
			integrations = append(integrations, models.Integration{
				Name:                      integration.Name(),
				SendResolved:              integration.SendResolved(),
				LastNotifyAttempt:         strfmt.DateTime(ts),
				LastNotifyAttemptDuration: d.String(),
				LastNotifyAttemptError: func() string {
					if err != nil {
						return err.Error()
					}
					return ""
				}(),
			})
		}

		apiReceivers = append(apiReceivers, models.Receiver{
			Active:       rcv.Active(),
			Integrations: integrations,
			Name:         rcv.Name(),
		})
	}

	return apiReceivers
}

// job contains all metadata required to test a receiver
type job struct {
	Config       *GrafanaIntegrationConfig
	ReceiverName string
	Notifier     notify.Notifier
}

// result contains the receiver that was tested and a non-nil error if the test failed
type result struct {
	Config       *GrafanaIntegrationConfig
	ReceiverName string
	Error        error
}

func newTestReceiversResult(alert types.Alert, results []result, receivers []*APIReceiver, notifiedAt time.Time) (*TestReceiversResult, int) {
	var numBadRequests, numTimeouts, numUnknownErrors int

	m := make(map[string]TestReceiverResult)
	for _, receiver := range receivers {
		// Set up the result for this receiver
		m[receiver.Name] = TestReceiverResult{
			Name: receiver.Name,
			// A Grafana receiver can have multiple nested receivers
			Configs: make([]TestIntegrationConfigResult, 0, len(receiver.Integrations)),
		}
	}
	for _, next := range results {
		tmp := m[next.ReceiverName]
		status := "ok"
		if next.Error != nil {
			status = "failed"
		}

		var invalidReceiverErr IntegrationValidationError
		var receiverTimeoutErr IntegrationTimeoutError

		var errString string
		err := ProcessIntegrationError(next.Config, next.Error)
		if err != nil {
			if errors.As(err, &invalidReceiverErr) {
				numBadRequests++
			} else if errors.As(err, &receiverTimeoutErr) {
				numTimeouts++
			} else {
				numUnknownErrors++
			}

			errString = err.Error()
		}

		tmp.Configs = append(tmp.Configs, TestIntegrationConfigResult{
			Name:   next.Config.Name,
			UID:    next.Config.UID,
			Status: status,
			Error:  errString,
		})
		m[next.ReceiverName] = tmp
	}
	v := new(TestReceiversResult)
	v.Alert = alert
	v.Receivers = make([]TestReceiverResult, 0, len(receivers))
	v.NotifedAt = notifiedAt
	for _, next := range m {
		v.Receivers = append(v.Receivers, next)
	}

	// Make sure the return order is deterministic.
	sort.Slice(v.Receivers, func(i, j int) bool {
		return v.Receivers[i].Name < v.Receivers[j].Name
	})

	var returnCode int
	if numBadRequests == len(v.Receivers) {
		// if all receivers contain invalid configuration
		returnCode = http.StatusBadRequest
	} else if numTimeouts == len(v.Receivers) {
		// if all receivers contain valid configuration but timed out
		returnCode = http.StatusRequestTimeout
	} else if numBadRequests+numTimeouts+numUnknownErrors > 0 {
		returnCode = http.StatusMultiStatus
	} else {
		// all receivers were sent a notification without error
		returnCode = http.StatusOK
	}

	return v, returnCode
}

func TestReceivers(
	ctx context.Context,
	c TestReceiversConfigBodyParams,
	tmpls []string,
	buildIntegrationsFunc func(*APIReceiver, *template.Template) ([]*nfstatus.Integration, error),
	externalURL string) (*TestReceiversResult, int, error) {

	now := time.Now() // The start time of the test
	testAlert := newTestAlert(c, now, now)

	tmpl, err := templateFromContent(tmpls, externalURL)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get template: %w", err)
	}

	// All invalid receiver configurations
	invalid := make([]result, 0, len(c.Receivers))
	// All receivers that need to be sent test notifications
	jobs := make([]job, 0, len(c.Receivers))

	for _, receiver := range c.Receivers {
		for _, intg := range receiver.Integrations {
			// Create an APIReceiver with a single integration so we
			// can identify invalid receiver integration configs
			singleIntReceiver := &APIReceiver{
				GrafanaIntegrations: GrafanaIntegrations{
					Integrations: []*GrafanaIntegrationConfig{intg},
				},
			}
			integrations, err := buildIntegrationsFunc(singleIntReceiver, tmpl)
			if err != nil || len(integrations) == 0 {
				invalid = append(invalid, result{
					Config:       intg,
					ReceiverName: intg.Name,
					Error:        err,
				})
			} else {
				jobs = append(jobs, job{
					Config:       intg,
					ReceiverName: receiver.Name,
					Notifier:     integrations[0],
				})
			}
		}
	}

	if len(invalid)+len(jobs) == 0 {
		return nil, 0, ErrNoReceivers
	}

	if len(jobs) == 0 {
		res, status := newTestReceiversResult(testAlert, invalid, c.Receivers, now)
		return res, status, nil
	}

	numWorkers := maxTestReceiversWorkers
	if numWorkers > len(jobs) {
		numWorkers = len(jobs)
	}

	resultCh := make(chan result, len(jobs))
	workCh := make(chan job, len(jobs))
	for _, job := range jobs {
		workCh <- job
	}
	close(workCh)

	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < numWorkers; i++ {
		g.Go(func() error {
			for next := range workCh {
				ctx = notify.WithGroupKey(ctx, fmt.Sprintf("%s-%s-%d", next.ReceiverName, testAlert.Labels.Fingerprint(), now.Unix()))
				ctx = notify.WithGroupLabels(ctx, testAlert.Labels)
				ctx = notify.WithReceiverName(ctx, next.ReceiverName)
				v := result{
					Config:       next.Config,
					ReceiverName: next.ReceiverName,
				}
				if _, err := next.Notifier.Notify(ctx, &testAlert); err != nil {
					v.Error = err
				}
				resultCh <- v
			}
			return nil
		})
	}

	err = g.Wait()
	close(resultCh)

	if err != nil {
		return nil, 0, err
	}

	results := make([]result, 0, len(jobs))
	for next := range resultCh {
		results = append(results, next)
	}

	res, status := newTestReceiversResult(testAlert, append(invalid, results...), c.Receivers, now)
	return res, status, nil
}

func TestTemplate(ctx context.Context, c TestTemplatesConfigBodyParams, tmpls []templates.TemplateDefinition, externalURL string, logger log.Logger) (*TestTemplatesResults, error) {
	definitions, err := parseTestTemplate(c.Name, c.Template)
	if err != nil {
		return &TestTemplatesResults{
			Errors: []TestTemplatesErrorResult{{
				Kind:  InvalidTemplate,
				Error: err.Error(),
			}},
		}, nil
	}

	// Recreate the current template replacing the definition blocks that are being tested. This is so that any blocks that were removed don't get defined.
	var found bool
	templateContents := make([]string, 0, len(tmpls)+1)
	for _, td := range tmpls {
		if td.Name == c.Name {
			// Template already exists, test with the new definition replacing the old one.
			templateContents = append(templateContents, c.Template)
			found = true
			continue
		}
		templateContents = append(templateContents, td.Template)
	}

	if !found {
		// Template is a new one, add it to the list.
		templateContents = append(templateContents, c.Template)
	}

	// Capture the underlying text template so we can use ExecuteTemplate.
	var newTextTmpl *tmpltext.Template
	var captureTemplate template.Option = func(text *tmpltext.Template, _ *tmplhtml.Template) {
		newTextTmpl = text
	}
	newTmpl, err := templateFromContent(templateContents, externalURL, captureTemplate)
	if err != nil {
		return nil, err
	}

	// Prepare the context.
	alerts := OpenAPIAlertsToAlerts(c.Alerts)
	ctx = notify.WithReceiverName(ctx, DefaultReceiverName)
	ctx = notify.WithGroupLabels(ctx, model.LabelSet{DefaultGroupLabel: DefaultGroupLabelValue})

	promTmplData := notify.GetTemplateData(ctx, newTmpl, alerts, logger)
	data := templates.ExtendData(promTmplData, logger)

	// Iterate over each definition in the template and evaluate it.
	var results TestTemplatesResults
	for _, def := range definitions {
		var buf bytes.Buffer
		err := newTextTmpl.ExecuteTemplate(&buf, def, data)
		if err != nil {
			results.Errors = append(results.Errors, TestTemplatesErrorResult{
				Name:  def,
				Kind:  ExecutionError,
				Error: err.Error(),
			})
		} else {
			results.Results = append(results.Results, TestTemplatesResult{
				Name: def,
				Text: buf.String(),
			})
		}
	}

	return &results, nil
}

func (am *GrafanaAlertmanager) ExternalURL() string {
	return am.externalURL
}

// ConfigHash returns the hash of the current running configuration.
// It is not safe to call without a lock.
func (am *GrafanaAlertmanager) ConfigHash() [16]byte {
	return am.configHash
}

func (am *GrafanaAlertmanager) WithReadLock(fn func()) {
	am.reloadConfigMtx.RLock()
	defer am.reloadConfigMtx.RUnlock()
	fn()
}

func (am *GrafanaAlertmanager) WithLock(fn func()) {
	am.reloadConfigMtx.Lock()
	defer am.reloadConfigMtx.Unlock()
	fn()
}

func (am *GrafanaAlertmanager) buildTimeIntervals(timeIntervals []config.TimeInterval, muteTimeIntervals []config.MuteTimeInterval) map[string][]timeinterval.TimeInterval {
	muteTimes := make(map[string][]timeinterval.TimeInterval, len(timeIntervals)+len(muteTimeIntervals))
	for _, ti := range timeIntervals {
		muteTimes[ti.Name] = ti.TimeIntervals
	}
	for _, ti := range muteTimeIntervals {
		muteTimes[ti.Name] = ti.TimeIntervals
	}
	return muteTimes
}

// ApplyConfig applies a new configuration by re-initializing all components using the configuration provided.
// It is not safe to call concurrently.
func (am *GrafanaAlertmanager) ApplyConfig(cfg Configuration) (err error) {
	am.templates = cfg.Templates()

	seen := make(map[string]struct{})
	tmpls := make([]string, 0, len(am.templates))
	for _, tc := range am.templates {
		if _, ok := seen[tc.Name]; ok {
			level.Warn(am.logger).Log("msg", "template with same name is defined multiple times, skipping...", "template_name", tc.Name)
			continue
		}
		tmpls = append(tmpls, tc.Template)
		seen[tc.Name] = struct{}{}
	}

	tmpl, err := templateFromContent(tmpls, am.ExternalURL())
	if err != nil {
		return err
	}

	// Finally, build the integrations map using the receiver configuration and templates.
	apiReceivers := cfg.Receivers()
	integrationsMap := make(map[string][]*Integration, len(apiReceivers))
	for _, apiReceiver := range apiReceivers {
		integrations, err := cfg.BuildReceiverIntegrationsFunc()(apiReceiver, tmpl)
		if err != nil {
			return err
		}
		integrationsMap[apiReceiver.Name] = integrations
	}

	// Now, let's put together our notification pipeline
	routingStage := make(notify.RoutingStage, len(integrationsMap))

	if am.inhibitor != nil {
		am.inhibitor.Stop()
	}
	if am.dispatcher != nil {
		am.dispatcher.Stop()
	}

	am.inhibitor = inhibit.NewInhibitor(am.alerts, cfg.InhibitRules(), am.marker, am.logger)
	am.timeIntervals = am.buildTimeIntervals(cfg.TimeIntervals(), cfg.MuteTimeIntervals())
	am.silencer = silence.NewSilencer(am.silences, am.marker, am.logger)

	meshStage := notify.NewGossipSettleStage(am.peer)
	inhibitionStage := notify.NewMuteStage(am.inhibitor, am.stageMetrics)
	timeMuteStage := notify.NewTimeMuteStage(timeinterval.NewIntervener(am.timeIntervals), am.stageMetrics)
	silencingStage := notify.NewMuteStage(am.silencer, am.stageMetrics)

	am.route = dispatch.NewRoute(cfg.RoutingTree(), nil)
	am.dispatcher = dispatch.NewDispatcher(am.alerts, am.route, routingStage, am.marker, am.timeoutFunc, cfg.DispatcherLimits(), am.logger, am.dispatcherMetrics)

	// TODO: This has not been upstreamed yet. Should be aligned when https://github.com/prometheus/alertmanager/pull/3016 is merged.
	var receivers []*nfstatus.Receiver
	activeReceivers := GetActiveReceiversMap(am.route)
	for name := range integrationsMap {
		stage := am.createReceiverStage(name, nfstatus.GetIntegrations(integrationsMap[name]), am.waitFunc, am.notificationLog)
		routingStage[name] = notify.MultiStage{meshStage, silencingStage, timeMuteStage, inhibitionStage, stage}
		_, isActive := activeReceivers[name]

		receivers = append(receivers, nfstatus.NewReceiver(name, isActive, integrationsMap[name]))
	}

	am.setReceiverMetrics(receivers, len(activeReceivers))
	am.setInhibitionRulesMetrics(cfg.InhibitRules())

	am.receivers = receivers
	am.buildReceiverIntegrationsFunc = cfg.BuildReceiverIntegrationsFunc()

	am.wg.Add(1)
	go func() {
		defer am.wg.Done()
		am.dispatcher.Run()
	}()

	am.wg.Add(1)
	go func() {
		defer am.wg.Done()
		am.inhibitor.Run()
	}()

	am.configHash = cfg.Hash()
	am.config = cfg.Raw()

	return nil
}

func (am *GrafanaAlertmanager) setInhibitionRulesMetrics(r []InhibitRule) {
	am.Metrics.configuredInhibitionRules.WithLabelValues(am.tenantString()).Set(float64(len(r)))
}

func (am *GrafanaAlertmanager) setReceiverMetrics(receivers []*nfstatus.Receiver, countActiveReceivers int) {
	am.Metrics.configuredReceivers.WithLabelValues(am.tenantString(), ActiveStateLabelValue).Set(float64(countActiveReceivers))
	am.Metrics.configuredReceivers.WithLabelValues(am.tenantString(), InactiveStateLabelValue).Set(float64(len(receivers) - countActiveReceivers))

	integrationsByType := make(map[string]int, len(receivers))
	for _, r := range receivers {
		for _, i := range r.Integrations() {
			integrationsByType[i.Name()]++
		}
	}

	for t, count := range integrationsByType {
		am.Metrics.configuredIntegrations.WithLabelValues(am.tenantString(), t).Set(float64(count))
	}
}

// PutAlerts receives the alerts and then sends them through the corresponding route based on whenever the alert has a receiver embedded or not
func (am *GrafanaAlertmanager) PutAlerts(postableAlerts amv2.PostableAlerts) error {
	now := time.Now()
	alerts, validationErr := PostableAlertsToAlertmanagerAlerts(postableAlerts, now)

	// Register metrics.
	for _, a := range alerts {
		if a.EndsAt.After(now) {
			am.Metrics.Firing().Inc()
		} else {
			am.Metrics.Resolved().Inc()
		}

		level.Debug(am.logger).Log("msg",
			"Putting alert",
			"alert",
			a,
			"starts_at",
			a.StartsAt,
			"ends_at",
			a.EndsAt)
	}

	if err := am.alerts.Put(alerts...); err != nil {
		// Notification sending alert takes precedence over validation errors.
		return err
	}
	if validationErr != nil {
		am.Metrics.Invalid().Add(float64(len(validationErr.Alerts)))
		// Even if validationErr is nil, the require.NoError fails on it.
		return validationErr
	}
	return nil
}

// PostableAlertsToAlertmanagerAlerts converts the PostableAlerts to a slice of *types.Alert.
// It sets `StartsAt` and `EndsAt`, ignores empty and namespace UID labels, and captures validation errors for each skipped alert.
func PostableAlertsToAlertmanagerAlerts(postableAlerts amv2.PostableAlerts, now time.Time) ([]*types.Alert, *AlertValidationError) {
	alerts := make([]*types.Alert, 0, len(postableAlerts))
	var validationErr *AlertValidationError
	for _, a := range postableAlerts {
		alert := &types.Alert{
			Alert: model.Alert{
				Labels:       model.LabelSet{},
				Annotations:  model.LabelSet{},
				StartsAt:     time.Time(a.StartsAt),
				EndsAt:       time.Time(a.EndsAt),
				GeneratorURL: a.GeneratorURL.String(),
			},
			UpdatedAt: now,
		}

		for k, v := range a.Labels {
			if len(v) == 0 || k == models.NamespaceUIDLabel { // Skip empty and namespace UID labels.
				continue
			}

			alert.Alert.Labels[model.LabelName(k)] = model.LabelValue(v)
		}

		for k, v := range a.Annotations {
			if len(v) == 0 { // Skip empty annotation.
				continue
			}
			alert.Alert.Annotations[model.LabelName(k)] = model.LabelValue(v)
		}

		// Ensure StartsAt is set.
		if alert.StartsAt.IsZero() {
			if alert.EndsAt.IsZero() {
				alert.StartsAt = now
			} else {
				alert.StartsAt = alert.EndsAt
			}
		}
		// If no end time is defined, set a timeout after which an alert
		// is marked resolved if it is not updated.
		if alert.EndsAt.IsZero() {
			alert.Timeout = true
			alert.EndsAt = now.Add(defaultResolveTimeout)
		}

		if err := alert.Validate(); err != nil {
			if validationErr == nil {
				validationErr = &AlertValidationError{}
			}
			validationErr.Alerts = append(validationErr.Alerts, a)
			validationErr.Errors = append(validationErr.Errors, err)
			continue
		}

		alerts = append(alerts, alert)
	}

	return alerts, validationErr
}

// AlertValidationError is the error capturing the validation errors
// faced on the alerts.
type AlertValidationError struct {
	Alerts amv2.PostableAlerts
	Errors []error // Errors[i] refers to Alerts[i].
}

func (e AlertValidationError) Error() string {
	errMsg := ""
	if len(e.Errors) != 0 {
		errMsg = e.Errors[0].Error()
		for _, e := range e.Errors[1:] {
			errMsg += ";" + e.Error()
		}
	}
	return errMsg
}

// createReceiverStage creates a pipeline of stages for a receiver.
func (am *GrafanaAlertmanager) createReceiverStage(name string, integrations []*notify.Integration, wait func() time.Duration, notificationLog notify.NotificationLog) notify.Stage {
	var fs notify.FanoutStage
	for i := range integrations {
		recv := &nflogpb.Receiver{
			GroupName:   name,
			Integration: integrations[i].Name(),
			Idx:         uint32(integrations[i].Index()),
		}
		var s notify.MultiStage
		s = append(s, notify.NewWaitStage(wait))
		s = append(s, notify.NewDedupStage(integrations[i], notificationLog, recv))
		s = append(s, notify.NewRetryStage(integrations[i], name, am.stageMetrics))
		s = append(s, notify.NewSetNotifiesStage(notificationLog, recv))

		fs = append(fs, s)
	}
	return fs
}

func (am *GrafanaAlertmanager) waitFunc() time.Duration {
	return time.Duration(am.peer.Position()) * am.peerTimeout
}

func (am *GrafanaAlertmanager) timeoutFunc(d time.Duration) time.Duration {
	// time.Duration d relates to the receiver's group_interval. Even with a group interval of 1s,
	// we need to make sure (non-position-0) peers in the cluster wait before flushing the notifications.
	if d < notify.MinTimeout {
		d = notify.MinTimeout
	}
	return d + am.waitFunc()
}

func (am *GrafanaAlertmanager) tenantString() string {
	return fmt.Sprintf("%d", am.tenantID)
}
