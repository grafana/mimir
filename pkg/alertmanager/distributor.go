// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/alertmanager/distributor.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package alertmanager

import (
	"context"
	"hash/fnv"
	"io"
	"math/rand"
	"net/http"
	"path"
	"strings"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/ring/client"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/alertmanager/merger"
	"github.com/grafana/mimir/pkg/util"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

// Distributor forwards requests to individual alertmanagers.
type Distributor struct {
	services.Service

	cfg              ClientConfig
	maxRecvMsgSize   int64
	requestsInFlight sync.WaitGroup

	alertmanagerRing        ring.ReadRing
	alertmanagerClientsPool ClientsPool

	logger log.Logger
}

// NewDistributor constructs a new Distributor
func NewDistributor(cfg ClientConfig, maxRecvMsgSize int64, alertmanagersRing *ring.Ring, alertmanagerClientsPool ClientsPool, logger log.Logger, reg prometheus.Registerer) (d *Distributor, err error) {
	if alertmanagerClientsPool == nil {
		alertmanagerClientsPool = newAlertmanagerClientsPool(client.NewRingServiceDiscovery(alertmanagersRing), cfg, logger, reg)
	}

	d = &Distributor{
		cfg:                     cfg,
		logger:                  logger,
		maxRecvMsgSize:          maxRecvMsgSize,
		alertmanagerRing:        alertmanagersRing,
		alertmanagerClientsPool: alertmanagerClientsPool,
	}

	d.Service = services.NewBasicService(nil, d.running, nil)
	return d, nil
}

func (d *Distributor) running(ctx context.Context) error {
	<-ctx.Done()
	d.requestsInFlight.Wait()
	return nil
}

func (d *Distributor) isQuorumWritePath(p string) bool {
	return strings.HasSuffix(p, "/alerts")
}

func (d *Distributor) isUnaryWritePath(p string) bool {
	if strings.HasSuffix(p, "/silences") {
		return true
	}
	if strings.HasSuffix(p, "/api/v1/grafana/templates/test") {
		return true
	}
	if strings.HasSuffix(p, "/api/v1/grafana/receivers/test") {
		return true
	}
	return false
}

func (d *Distributor) isUnaryDeletePath(p string) bool {
	return strings.HasSuffix(path.Dir(p), "/silence")
}

func (d *Distributor) isQuorumReadPath(p string) (bool, merger.Merger) {
	if strings.HasSuffix(p, "/v2/alerts") {
		return true, merger.V2Alerts{}
	}
	if strings.HasSuffix(p, "/v2/alerts/groups") {
		return true, merger.V2AlertGroups{}
	}
	if strings.HasSuffix(p, "/v2/silences") {
		return true, merger.V2Silences{}
	}
	if strings.HasSuffix(path.Dir(p), "/v2/silence") {
		return true, merger.V2SilenceID{}
	}
	return false, nil
}

func (d *Distributor) isAllReadPath(p string) (bool, merger.Merger) {
	// When querying receivers status, we need to wait for responses from all three replicas
	// for best quality results. This is because any of the three could have sent a
	// notification. This does mean that some requests might fail if a replica is unavailable
	// but it has not left the ring or been deemed Unhealthy yet.
	if strings.HasSuffix(p, "/api/v1/grafana/receivers") {
		return true, merger.ExperimentalReceivers{}
	}
	return false, nil
}

// DistributeRequest shards the writes and returns as soon as the quorum is satisfied.
// In case of reads, it proxies the request to one of the alertmanagers.
func (d *Distributor) DistributeRequest(w http.ResponseWriter, r *http.Request) {
	d.requestsInFlight.Add(1)
	defer d.requestsInFlight.Done()

	userID, err := tenant.TenantID(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	logger := util_log.WithContext(r.Context(), d.logger)

	if r.Method == http.MethodPost {
		if d.isQuorumWritePath(r.URL.Path) {
			d.doQuorum(userID, w, r, logger, merger.Noop{})
			return
		}
		if d.isUnaryWritePath(r.URL.Path) {
			d.doUnary(userID, w, r, logger)
			return
		}
	}
	if r.Method == http.MethodDelete {
		if d.isUnaryDeletePath(r.URL.Path) {
			d.doUnary(userID, w, r, logger)
			return
		}
	}
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		if ok, m := d.isQuorumReadPath(r.URL.Path); ok {
			d.doQuorum(userID, w, r, logger, m)
			return
		}
		if ok, m := d.isAllReadPath(r.URL.Path); ok {
			d.doAll(userID, w, r, logger, m)
			return
		}

		// All other paths are just passed to a random replica.
		// This is primarily used for serving the web user interface.
		d.doUnary(userID, w, r, logger)
		return
	}

	http.Error(w, "route not supported by distributor", http.StatusNotFound)
}

func (d *Distributor) doAll(userID string, w http.ResponseWriter, r *http.Request, logger log.Logger, m merger.Merger) {
	var body []byte
	var err error
	if r.Body != nil {
		body, err = io.ReadAll(http.MaxBytesReader(w, r.Body, d.maxRecvMsgSize))
		if err != nil {
			if util.IsRequestBodyTooLarge(err) {
				http.Error(w, "Request body too large", http.StatusRequestEntityTooLarge)
				return
			}
			level.Error(logger).Log("msg", "failed to read the request body during write", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	grpcHeaders := httpToHttpgrpcHeaders(r.Header)

	// Only get the set of replicas which contain the specified user.
	key := shardByUser(userID)
	replicationSet, err := d.alertmanagerRing.Get(key, RingOp, nil, nil, nil)
	if err != nil {
		respondFromError(err, w, logger)
		return
	}

	// Force waiting for responses from all healthy replicas.
	replicationSet.MaxErrors = 0

	results, err := replicationSet.Do(r.Context(), 0, func(ctx context.Context, instance *ring.InstanceDesc) (any, error) {
		ctx = user.InjectOrgID(ctx, userID)
		sp, ctx := opentracing.StartSpanFromContext(ctx, "Distributor.doAll")
		defer sp.Finish()

		resp, err := d.doRequest(ctx, *instance, &httpgrpc.HTTPRequest{
			Method:  r.Method,
			Url:     r.RequestURI,
			Body:    body,
			Headers: grpcHeaders,
		})
		if err != nil {
			return nil, err
		}

		if resp.Code/100 != 2 {
			return nil, httpgrpc.ErrorFromHTTPResponse(resp)
		}

		return resp, nil
	})

	if err != nil {
		respondFromError(err, w, logger)
		return
	}

	responses := make([]*httpgrpc.HTTPResponse, len(results))
	for i := range results {
		responses[i] = results[i].(*httpgrpc.HTTPResponse)
	}

	if len(results) > 0 {
		respondFromMultipleHTTPGRPCResponses(w, logger, responses, m)
	} else {
		// This should not happen.
		level.Error(logger).Log("msg", "distributor did not receive any response from alertmanagers, but there were no errors")
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (d *Distributor) doQuorum(userID string, w http.ResponseWriter, r *http.Request, logger log.Logger, m merger.Merger) {
	var body []byte
	var err error
	if r.Body != nil {
		body, err = io.ReadAll(http.MaxBytesReader(w, r.Body, d.maxRecvMsgSize))
		if err != nil {
			if util.IsRequestBodyTooLarge(err) {
				http.Error(w, "Request body too large", http.StatusRequestEntityTooLarge)
				return
			}
			level.Error(logger).Log("msg", "failed to read the request body during write", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	var responses []*httpgrpc.HTTPResponse
	var responsesMtx sync.Mutex
	grpcHeaders := httpToHttpgrpcHeaders(r.Header)
	err = ring.DoBatchWithOptions(r.Context(), RingOp, d.alertmanagerRing, []uint32{shardByUser(userID)}, func(am ring.InstanceDesc, _ []int) error {
		// Use a background context to make sure all alertmanagers get the request even if we return early.
		localCtx := user.InjectOrgID(context.Background(), userID)
		sp, localCtx := opentracing.StartSpanFromContext(localCtx, "Distributor.doQuorum")
		defer sp.Finish()

		resp, err := d.doRequest(localCtx, am, &httpgrpc.HTTPRequest{
			Method:  r.Method,
			Url:     r.RequestURI,
			Body:    body,
			Headers: grpcHeaders,
		})
		if err != nil {
			return err
		}

		if resp.Code/100 != 2 {
			return httpgrpc.ErrorFromHTTPResponse(resp)
		}

		responsesMtx.Lock()
		responses = append(responses, resp)
		responsesMtx.Unlock()

		return nil
	}, ring.DoBatchOptions{})

	if err != nil {
		respondFromError(err, w, logger)
		return
	}

	responsesMtx.Lock() // Another request might be ongoing after quorum.
	resps := responses
	responsesMtx.Unlock()

	if len(resps) > 0 {
		respondFromMultipleHTTPGRPCResponses(w, logger, resps, m)
	} else {
		// This should not happen.
		level.Error(logger).Log("msg", "distributor did not receive any response from alertmanagers, but there were no errors")
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (d *Distributor) doUnary(userID string, w http.ResponseWriter, r *http.Request, logger log.Logger) {
	key := shardByUser(userID)
	replicationSet, err := d.alertmanagerRing.Get(key, RingOp, nil, nil, nil)
	if err != nil {
		level.Error(logger).Log("msg", "failed to get replication set from the ring", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, d.maxRecvMsgSize))
	if err != nil {
		if util.IsRequestBodyTooLarge(err) {
			http.Error(w, "Request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		level.Error(logger).Log("msg", "failed to read the request body during read", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	req := &httpgrpc.HTTPRequest{
		Method:  r.Method,
		Url:     r.RequestURI,
		Body:    body,
		Headers: httpToHttpgrpcHeaders(r.Header),
	}

	sp, ctx := opentracing.StartSpanFromContext(r.Context(), "Distributor.doUnary")
	defer sp.Finish()
	// Until we have a mechanism to combine the results from multiple alertmanagers,
	// we forward the request to only one of the alertmanagers.
	amDesc := replicationSet.Instances[rand.Intn(len(replicationSet.Instances))]
	resp, err := d.doRequest(ctx, amDesc, req)
	if err != nil {
		respondFromError(err, w, logger)
		return
	}

	respondFromHTTPGRPCResponse(w, resp)
}

func respondFromError(err error, w http.ResponseWriter, logger log.Logger) {
	httpResp, ok := httpgrpc.HTTPResponseFromError(err)
	if !ok {
		level.Error(logger).Log("msg", "failed to process the request to the alertmanager", "err", err)
		http.Error(w, "Failed to process the request to the alertmanager", http.StatusInternalServerError)
		return
	}
	respondFromHTTPGRPCResponse(w, httpResp)
}

func respondFromHTTPGRPCResponse(w http.ResponseWriter, httpResp *httpgrpc.HTTPResponse) {
	for _, h := range httpResp.Headers {
		for _, v := range h.Values {
			w.Header().Add(h.Key, v)
		}
	}
	w.WriteHeader(int(httpResp.Code))
	w.Write(httpResp.Body) //nolint
}

func (d *Distributor) doRequest(ctx context.Context, am ring.InstanceDesc, req *httpgrpc.HTTPRequest) (*httpgrpc.HTTPResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, d.cfg.RemoteTimeout)
	defer cancel()
	amClient, err := d.alertmanagerClientsPool.GetClientFor(am.Addr)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get alertmanager client from pool (alertmanager address: %s)", am.Addr)
	}

	return amClient.HandleRequest(ctx, req)
}

func shardByUser(userID string) uint32 {
	ringHasher := fnv.New32a()
	// Hasher never returns err.
	_, _ = ringHasher.Write([]byte(userID))
	return ringHasher.Sum32()
}

func httpToHttpgrpcHeaders(hs http.Header) []*httpgrpc.Header {
	result := make([]*httpgrpc.Header, 0, len(hs))
	for k, vs := range hs {
		result = append(result, &httpgrpc.Header{
			Key:    k,
			Values: vs,
		})
	}
	return result
}

func respondFromMultipleHTTPGRPCResponses(w http.ResponseWriter, logger log.Logger, responses []*httpgrpc.HTTPResponse, merger merger.Merger) {
	bodies := make([][]byte, len(responses))
	for i, r := range responses {
		bodies[i] = r.Body
	}

	body, err := merger.MergeResponses(bodies)
	if err != nil {
		level.Error(logger).Log("msg", "failed to merge responses for request", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// It is assumed by using this function, the caller knows that the responses it receives
	// have already been checked for success or failure, and that the headers will always
	// match due to the nature of the request. If this is not the case, a different merge
	// function should be implemented to cope with the differing responses.
	response := &httpgrpc.HTTPResponse{
		Code:    responses[0].Code,
		Headers: responses[0].Headers,
		Body:    body,
	}

	respondFromHTTPGRPCResponse(w, response)
}
