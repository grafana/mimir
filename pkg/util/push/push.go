// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/push/push.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package push

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/middleware"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/log"
)

const (
	ReplicaHeader              = "X-Prometheus-HA-Replica"
	ClusterHeader              = "X-Prometheus-HA-Cluster"
	IsSecondaryRWReplicaHeader = "X-Prometheus-Secondary-Remote-Write-Replica"
)

// Func defines the type of the push. It is similar to http.HandlerFunc.
type Func func(ctx context.Context, req *Request) (*mimirpb.WriteResponse, error)

// parserFunc defines how to read the body the request from an HTTP request
type parserFunc func(ctx context.Context, r *http.Request, maxSize int, buffer []byte, req *mimirpb.PreallocWriteRequest) ([]byte, error)

// Wrap a slice in a struct so we can store a pointer in sync.Pool
type bufHolder struct {
	buf []byte
}

var bufferPool = sync.Pool{
	New: func() interface{} { return &bufHolder{buf: make([]byte, 256*1024)} },
}

const SkipLabelNameValidationHeader = "X-Mimir-SkipLabelNameValidation"
const statusClientClosedRequest = 499

// Handler is a http.Handler which accepts WriteRequests.
func Handler(
	maxRecvMsgSize int,
	sourceIPs *middleware.SourceIPExtractor,
	allowSkipLabelNameValidation bool,
	push Func,
	replicaChecker func(ctx context.Context, userID, cluster, replica string, now time.Time) error,
) http.Handler {
	return handler(maxRecvMsgSize, sourceIPs, allowSkipLabelNameValidation, push, func(ctx context.Context, r *http.Request, maxRecvMsgSize int, dst []byte, req *mimirpb.PreallocWriteRequest) ([]byte, error) {
		res, err := util.ParseProtoReader(ctx, r.Body, int(r.ContentLength), maxRecvMsgSize, dst, req, util.RawSnappy)
		if errors.Is(err, util.MsgSizeTooLargeErr{}) {
			err = distributorMaxWriteMessageSizeErr{actual: int(r.ContentLength), limit: maxRecvMsgSize}
		}
		return res, err
	}, replicaChecker)
}

type distributorMaxWriteMessageSizeErr struct {
	actual, limit int
}

func (e distributorMaxWriteMessageSizeErr) Error() string {
	msgSizeDesc := fmt.Sprintf(" of %d bytes", e.actual)
	if e.actual < 0 {
		msgSizeDesc = ""
	}
	return globalerror.DistributorMaxWriteMessageSize.MessageWithPerInstanceLimitConfig(fmt.Sprintf("the incoming push request has been rejected because its message size%s is larger than the allowed limit of %d bytes", msgSizeDesc, e.limit), "distributor.max-recv-msg-size")
}

func handler(maxRecvMsgSize int,
	sourceIPs *middleware.SourceIPExtractor,
	allowSkipLabelNameValidation bool,
	push Func,
	parser parserFunc,
	replicaChecker func(ctx context.Context, userID, cluster, replica string, now time.Time) error,
) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		logger := log.WithContext(ctx, log.Logger)
		if sourceIPs != nil {
			source := sourceIPs.Get(r)
			if source != "" {
				ctx = util.AddSourceIPsToOutgoingContext(ctx, source)
				logger = log.WithSourceIPs(source, logger)
			}
		}
		// TODO move this to middleware
		cluster, replica := r.Header.Get(ClusterHeader), r.Header.Get(ReplicaHeader)
		if cluster == "my-cluster" {
			level.Info(logger).Log("msg", "xxx Header checkers.....",
				"clusterHeader", cluster,
				"replicaHeader", replica,
			)
		}
		userID, err := tenant.TenantID(ctx)
		if cluster == "my-cluster" {
			level.Info(logger).Log("msg", "xxx Header checkers.....",
				"userID", userID,
				"clusterHeader", cluster,
				"replicaHeader", replica,
			)
		}
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if replica != "" && cluster != "" {
			err = replicaChecker(ctx, userID, cluster, replica, time.Now())
			if err != nil {
				if cluster == "my-cluster" {
					level.Info(logger).Log("msg", "xxx replica check is failed", "err", err.Error(),
						"userID", userID,
						"clusterHeader", cluster,
						"replicaHeader", replica,
					)
				}
				// TODO add metrics on success or failure in HA replica check
				// TODO make sure the errrr is replica not match
				// if errors.Is(err, replicasNotMatchError{}) {
				if true {
					// TODO: the header is secondary replica
					// we stop this
					w.WriteHeader(http.StatusAccepted)
					w.Header().Set(IsSecondaryRWReplicaHeader, "true")
				}
				return
			}
			if cluster == "my-cluster" {
				level.Info(logger).Log("msg", "xxx replica check run successfully",
					"userID", userID,
					"clusterHeader", cluster,
					"replicaHeader", replica,
				)
			}
			// propagate HA check
		} else {
			if cluster == "my-cluster" {
				level.Info(logger).Log("msg", "xxx no ha header found",
					"userID", userID,
					"clusterHeader", cluster,
					"replicaHeader", replica,
				)
			}
		}

		supplier := func() (*mimirpb.WriteRequest, func(), error) {
			if cluster == "my-cluster" {
				level.Info(logger).Log("msg", "xxx parsing request...",
					"userID", userID,
					"clusterHeader", cluster,
					"replicaHeader", replica,
				)
			}
			bufHolder := bufferPool.Get().(*bufHolder)
			var req mimirpb.PreallocWriteRequest
			buf, err := parser(ctx, r, maxRecvMsgSize, bufHolder.buf, &req)
			if err != nil {
				// Check for httpgrpc error, default to client error if parsing failed
				if _, ok := httpgrpc.HTTPResponseFromError(err); !ok {
					err = httpgrpc.Errorf(http.StatusBadRequest, err.Error())
				}

				bufferPool.Put(bufHolder)
				return nil, nil, err
			}
			// If decoding allocated a bigger buffer, put that one back in the pool.
			if buf = buf[:cap(buf)]; len(buf) > len(bufHolder.buf) {
				bufHolder.buf = buf
			}

			if allowSkipLabelNameValidation {
				req.SkipLabelNameValidation = req.SkipLabelNameValidation && r.Header.Get(SkipLabelNameValidationHeader) == "true"
			} else {
				req.SkipLabelNameValidation = false
			}

			cleanup := func() {
				mimirpb.ReuseSlice(req.Timeseries)
				bufferPool.Put(bufHolder)
			}
			return &req.WriteRequest, cleanup, nil
		}
		req := newRequest(supplier)
		if _, err := push(ctx, req); err != nil {
			if errors.Is(err, context.Canceled) {
				http.Error(w, err.Error(), statusClientClosedRequest)
				level.Warn(logger).Log("msg", "push request canceled", "err", err)
				return
			}
			resp, ok := httpgrpc.HTTPResponseFromError(err)
			if !ok {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if resp.GetCode() != 202 {
				level.Error(logger).Log("msg", "push error", "err", err)
			}
			http.Error(w, string(resp.Body), int(resp.Code))
		}
	})
}
