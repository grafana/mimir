// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/api/handlers.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package api

import (
	"cmp"
	"context"
	"embed"
	"errors"
	"html/template"
	"net/http"
	"path"
	"slices"
	"strings"
	"sync"

	"github.com/go-kit/log"
	"github.com/gorilla/mux"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/regexp"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	v1 "github.com/prometheus/prometheus/web/api/v1"

	"github.com/grafana/mimir/pkg/querier"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/usagestats"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/limiter"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/propagation"
	"github.com/grafana/mimir/pkg/util/validation"
)

func newIndexPageContent(pathPrefix string) *IndexPageContent {
	return &IndexPageContent{
		PathPrefix: pathPrefix,
	}
}

// IndexPageContent is a map of sections to path -> description.
type IndexPageContent struct {
	mu         sync.Mutex
	PathPrefix string
	elements   []IndexPageLinkGroup
}

type IndexPageLinkGroup struct {
	weight int
	Desc   string
	Links  []IndexPageLink
}

type IndexPageLink struct {
	Desc      string
	Path      string
	Dangerous bool
}

// List of weights to order link groups in the same order as weights are ordered here.
const (
	serviceStatusWeight = iota
	configWeight
	runtimeConfigWeight
	defaultWeight
	memberlistWeight
	dangerousWeight
)

func (pc *IndexPageContent) AddLinks(weight int, groupDesc string, links []IndexPageLink) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	for i := range links {
		if strings.HasPrefix(links[i].Path, "/") {
			links[i].Path = pc.PathPrefix + links[i].Path
		}
	}

	// Append the links to the group if already existing.
	for i, group := range pc.elements {
		if group.Desc != groupDesc {
			continue
		}

		group.Links = append(group.Links, links...)
		pc.elements[i] = group
		return
	}

	// The group hasn't been found. We create a new one.
	pc.elements = append(pc.elements, IndexPageLinkGroup{weight: weight, Desc: groupDesc, Links: links})
}

func (pc *IndexPageContent) GetContent() []IndexPageLinkGroup {
	pc.mu.Lock()
	els := append([]IndexPageLinkGroup(nil), pc.elements...)
	pc.mu.Unlock()

	slices.SortFunc(els, func(a, b IndexPageLinkGroup) int {
		return cmp.Or(
			cmp.Compare(a.weight, b.weight),
			strings.Compare(a.Desc, b.Desc),
		)
	})

	return els
}

//go:embed index.gohtml
var indexPageHTML string

type indexPageContents struct {
	LinkGroups []IndexPageLinkGroup
}

//go:embed static
var staticFiles embed.FS

func indexHandler(content *IndexPageContent) http.HandlerFunc {
	templ := template.New("main")
	template.Must(templ.Parse(indexPageHTML))

	return func(w http.ResponseWriter, _ *http.Request) {
		err := templ.Execute(w, indexPageContents{LinkGroups: content.GetContent()})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

func (cfg *Config) configHandler(actualCfg interface{}, defaultCfg interface{}) http.HandlerFunc {
	if cfg.CustomConfigHandler != nil {
		return cfg.CustomConfigHandler(actualCfg, defaultCfg)
	}
	return DefaultConfigHandler(actualCfg, defaultCfg)
}

func DefaultConfigHandler(actualCfg interface{}, defaultCfg interface{}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var output interface{}
		switch r.URL.Query().Get("mode") {
		case "diff":
			defaultCfgObj, err := util.YAMLMarshalUnmarshal(defaultCfg)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			actualCfgObj, err := util.YAMLMarshalUnmarshal(actualCfg)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			diff, err := util.DiffConfig(defaultCfgObj, actualCfgObj)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			output = diff

		case "defaults":
			output = defaultCfg
		default:
			output = actualCfg
		}

		util.WriteYAMLResponse(w, output)
	}
}

type configResponse struct {
	Status string            `json:"status"`
	Config map[string]string `json:"data"`
}

func (cfg *Config) statusConfigHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		response := configResponse{
			Status: "success",
			Config: map[string]string{},
		}
		util.WriteJSONResponse(w, response)
	}
}

type flagsResponse struct {
	Status string            `json:"status"`
	Flags  map[string]string `json:"data"`
}

func (cfg *Config) statusFlagsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		response := flagsResponse{
			Status: "success",
			Flags:  map[string]string{},
		}
		util.WriteJSONResponse(w, response)
	}
}

// NewQuerierHandler returns a HTTP handler that can be used by the querier service to
// either register with the frontend worker query processor or with the external HTTP
// server to fulfill the Prometheus query API.
func NewQuerierHandler(
	cfg Config,
	querierCfg querier.Config,
	queryable storage.SampleAndChunkQueryable,
	exemplarQueryable storage.ExemplarQueryable,
	metadataSupplier querier.MetadataSupplier,
	engine promql.QueryEngine,
	distributor Distributor,
	metrics *querier.RequestMetrics,
	reg prometheus.Registerer,
	logger log.Logger,
	limits *validation.Overrides,
	extractor propagation.Extractor,
) http.Handler {
	const (
		remoteWriteEnabled = false
		otlpEnabled        = false
		appendMetadata     = false
	)

	api := v1.NewAPI(
		engine,
		querier.NewErrorTranslateSampleAndChunkQueryable(queryable), // Translate errors to errors expected by API.
		nil, // No remote write support.
		exemplarQueryable,
		func(context.Context) v1.ScrapePoolsRetriever { return &querier.DummyTargetRetriever{} },
		func(context.Context) v1.TargetRetriever { return &querier.DummyTargetRetriever{} },
		func(context.Context) v1.AlertmanagerRetriever { return &querier.DummyAlertmanagerRetriever{} },
		func() config.Config { return config.Config{} },
		map[string]string{}, // TODO: include configuration flags
		v1.GlobalURLOptions{},
		func(f http.HandlerFunc) http.HandlerFunc { return f },
		nil,   // Only needed for admin APIs.
		"",    // This is for snapshots, which is disabled when admin APIs are disabled. Hence empty.
		false, // Disable admin APIs.
		util_log.SlogFromGoKit(logger),
		func(context.Context) v1.RulesRetriever { return &querier.DummyRulesRetriever{} },
		0, 0, 0, // Remote read samples and concurrency limit.
		false, // Not an agent.
		regexp.MustCompile(".*"),
		func() (v1.RuntimeInfo, error) { return v1.RuntimeInfo{}, errors.New("not implemented") },
		&v1.PrometheusVersion{},
		nil,
		nil,
		// This is used for the stats API which we should not support. Or find other ways to.
		prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) { return nil, nil }),
		reg,
		querier.StatsRenderer,
		remoteWriteEnabled,
		nil,
		otlpEnabled,
		false,
		false,
		true,
		querierCfg.EngineConfig.LookbackDelta,
		false,
		appendMetadata,
		nil,
		nil,
	)

	api.InstallCodec(protobufCodec{})

	router := mux.NewRouter()
	routeInjector := middleware.RouteInjector{RouteMatcher: router}
	router.Use(routeInjector.Wrap, propagation.Middleware(extractor).Wrap)

	// Use a separate metric for the querier in order to differentiate requests from the query-frontend when
	// running Mimir in monolithic mode.
	instrumentMiddleware := middleware.Instrument{
		Duration:         metrics.RequestDuration,
		RequestBodySize:  metrics.ReceivedMessageSize,
		ResponseBodySize: metrics.SentMessageSize,
		InflightRequests: metrics.InflightRequests,
	}
	router.Use(instrumentMiddleware.Wrap)

	// Define the prefixes for all routes
	promPrefix := path.Join(cfg.ServerPrefix, cfg.PrometheusHTTPPrefix, "/api/v1")
	promRouter := route.New().WithPrefix(path.Join(promPrefix))
	api.Register(promRouter)

	// Track the requests count in the anonymous usage stats.
	remoteReadStats := usagestats.NewRequestsMiddleware("querier_remote_read_requests")
	instantQueryStats := usagestats.NewRequestsMiddleware("querier_instant_query_requests")
	rangeQueryStats := usagestats.NewRequestsMiddleware("querier_range_query_requests")
	exemplarsQueryStats := usagestats.NewRequestsMiddleware("querier_exemplars_query_requests")
	labelsQueryStats := usagestats.NewRequestsMiddleware("querier_labels_query_requests")
	seriesQueryStats := usagestats.NewRequestsMiddleware("querier_series_query_requests")
	metadataQueryStats := usagestats.NewRequestsMiddleware("querier_metadata_query_requests")
	cardinalityQueryStats := usagestats.NewRequestsMiddleware("querier_cardinality_query_requests")
	formattingQueryStats := usagestats.NewRequestsMiddleware("querier_formatting_requests")

	// Add memory consumption tracker in middleware for endpoints like /read and /series that need the memory consumption tracker to be available in the context.
	// We use memory consumption tracker intensively in streamingpromql and enforce per query memory limit there. Outside streamingpromql's request flow, the memory consumption tracker
	// still needs to be initiated, but the tracker will not enforce memory limit.
	// The middleware must be added here in querier instead of in query-frontend so that the context will be initialized with the memory consumption tracker
	// to be used in upstream Queryables or Queriers.
	unlimitedMemoryTrackerMiddleware := limiter.UnlimitedMemoryTrackerMiddleware{}

	// TODO(gotjosh): This custom handler is temporary until we're able to vendor the changes in:
	// https://github.com/prometheus/prometheus/pull/7125/files
	router.Path(path.Join(promPrefix, "/read")).Methods("POST").Handler(remoteReadStats.Wrap(unlimitedMemoryTrackerMiddleware.Wrap(querier.RemoteReadHandler(queryable, logger, querierCfg))))
	router.Path(path.Join(promPrefix, "/query")).Methods("GET", "POST").Handler(instantQueryStats.Wrap(promRouter))
	router.Path(path.Join(promPrefix, "/query_range")).Methods("GET", "POST").Handler(rangeQueryStats.Wrap(promRouter))
	router.Path(path.Join(promPrefix, "/query_exemplars")).Methods("GET", "POST").Handler(exemplarsQueryStats.Wrap(promRouter))
	router.Path(path.Join(promPrefix, "/labels")).Methods("GET", "POST").Handler(labelsQueryStats.Wrap(promRouter))
	router.Path(path.Join(promPrefix, "/label/{name}/values")).Methods("GET").Handler(labelsQueryStats.Wrap(promRouter))
	router.Path(path.Join(promPrefix, "/series")).Methods("GET", "POST", "DELETE").Handler(seriesQueryStats.Wrap(unlimitedMemoryTrackerMiddleware.Wrap(promRouter)))
	router.Path(path.Join(promPrefix, "/metadata")).Methods("GET").Handler(metadataQueryStats.Wrap(querier.NewMetadataHandler(metadataSupplier)))
	router.Path(path.Join(promPrefix, "/cardinality/label_names")).Methods("GET", "POST").Handler(cardinalityQueryStats.Wrap(querier.LabelNamesCardinalityHandler(distributor, limits)))
	router.Path(path.Join(promPrefix, "/cardinality/label_values")).Methods("GET", "POST").Handler(cardinalityQueryStats.Wrap(querier.LabelValuesCardinalityHandler(distributor, limits)))
	router.Path(path.Join(promPrefix, "/cardinality/active_series")).Methods("GET", "POST").Handler(cardinalityQueryStats.Wrap(querier.ActiveSeriesCardinalityHandler(distributor, limits)))
	router.Path(path.Join(promPrefix, "/cardinality/active_native_histogram_metrics")).Methods("GET", "POST").Handler(cardinalityQueryStats.Wrap(querier.ActiveNativeHistogramMetricsHandler(distributor, limits)))
	router.Path(path.Join(promPrefix, "/format_query")).Methods("GET", "POST").Handler(formattingQueryStats.Wrap(promRouter))

	// Track execution time.
	return stats.NewWallTimeMiddleware().Wrap(router)
}

//go:embed memberlist_status.gohtml
var memberlistStatusPageHTML string

func memberlistStatusHandler(kvs *memberlist.KVInitService) http.Handler {
	templ := template.New("memberlist_status")
	templ.Funcs(map[string]interface{}{
		"StringsJoin": strings.Join,
		"GetZoneFromMeta": func(meta []byte) string {
			return memberlist.EncodedNodeMetadata(meta).Zone()
		},
		"GetRoleFromMeta": func(meta []byte) string {
			return memberlist.EncodedNodeMetadata(meta).Role().String()
		},
	})
	template.Must(templ.Parse(memberlistStatusPageHTML))
	return memberlist.NewHTTPStatusHandler(kvs, templ)
}
