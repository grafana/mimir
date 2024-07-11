// SPDX-License-Identifier: AGPL-3.0-only

package api

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/grafana/dskit/middleware"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	//lint:ignore faillint Simple import for the API error package to format the error correctly.
	apierror "github.com/grafana/mimir/pkg/api/error"
)

type headerOptionsContextKeyType int

const headerOptionsContextKey headerOptionsContextKeyType = 1

type HeaderOptionsConfig struct {
	// Headers is a map of Headers to handle and the function to validate the value.
	// Headers are case insensitive.
	Headers map[string]func(string) error
}

type HeaderOptionsContext struct {
	headers map[string]string
}

func HeaderOptionsMiddleware(cfg HeaderOptionsConfig) middleware.Interface {
	return middleware.Func(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			hoc := HeaderOptionsContext{headers: make(map[string]string)}

			for header, validate := range cfg.Headers {
				value := r.Header.Get(header)
				if value == "" {
					continue
				}

				if err := validate(value); err != nil {
					// Send a Prometheus API-style JSON error response.
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusBadRequest)
					e := apierror.Newf(apierror.TypeBadData, "invalid value '%s' for '%s' header, %s", value, header, err.Error())

					if body, err := e.EncodeJSON(); err == nil {
						_, _ = w.Write(body)
					}

					return
				}

				hoc.headers[header] = value
			}
			ctx = ContextWithHeaderOptions(ctx, hoc)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	})
}

func HeaderOptionFromContext(ctx context.Context, header string) (string, bool) {
	hoc, ok := HeaderOptionsFromContext(ctx)
	if !ok {
		return "", false
	}
	value, ok := hoc.headers[header]
	return value, ok
}

func SetHeaderOptions(ctx context.Context, req *http.Request) {
	hoc, ok := HeaderOptionsFromContext(ctx)
	if !ok {
		return
	}
	for k, v := range hoc.headers {
		req.Header.Add(k, v)
	}
}

const headerOptionsGrpcMdKey = "__querier_api_header_options__"

func HeaderOptionsFromContext(ctx context.Context) (HeaderOptionsContext, bool) {
	hoc, ok := ctx.Value(headerOptionsContextKey).(HeaderOptionsContext)
	return hoc, ok
}

func ContextWithHeaderOptions(ctx context.Context, hoc HeaderOptionsContext) context.Context {
	prev, ok := HeaderOptionsFromContext(ctx)
	if ok {
		// Merge/overwrite with existing headers.
		for k, v := range hoc.headers {
			prev.headers[k] = v
		}
		return context.WithValue(ctx, headerOptionsContextKey, prev)
	}
	return context.WithValue(ctx, headerOptionsContextKey, hoc)
}

func ContextWithAHeaderOption(ctx context.Context, header, value string) context.Context {
	return ContextWithHeaderOptions(ctx, HeaderOptionsContext{headers: map[string]string{header: value}})
}

func HeaderOptionsClientUnaryInterceptor(ctx context.Context, method string, req any, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if c, ok := HeaderOptionsFromContext(ctx); ok {
		j, err := json.Marshal(c.headers)
		if err != nil {
			return err
		}
		ctx = metadata.AppendToOutgoingContext(ctx, headerOptionsGrpcMdKey, string(j))
	}
	return invoker(ctx, method, req, reply, cc, opts...)
}

func HeaderOptionsServerUnaryInterceptor(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	headerOptions := metadata.ValueFromIncomingContext(ctx, headerOptionsGrpcMdKey)
	if len(headerOptions) > 0 {
		hoc := HeaderOptionsContext{headers: make(map[string]string)}
		err := json.Unmarshal([]byte(headerOptions[0]), &(hoc.headers))
		if err != nil {
			return nil, err
		}
		ctx = ContextWithHeaderOptions(ctx, hoc)
	}
	return handler(ctx, req)
}

func HeaderOptionsClientStreamInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	if c, ok := HeaderOptionsFromContext(ctx); ok {
		j, err := json.Marshal(c.headers)
		if err != nil {
			return nil, err
		}
		ctx = metadata.AppendToOutgoingContext(ctx, headerOptionsGrpcMdKey, string(j))
	}
	return streamer(ctx, desc, cc, method, opts...)
}

func HeaderOptionsServerStreamInterceptor(srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	headerOptions := metadata.ValueFromIncomingContext(ss.Context(), headerOptionsGrpcMdKey)
	if len(headerOptions) > 0 {
		hoc := HeaderOptionsContext{headers: make(map[string]string)}
		err := json.Unmarshal([]byte(headerOptions[0]), &(hoc.headers))
		if err != nil {
			return err
		}
		ctx := ContextWithHeaderOptions(ss.Context(), hoc)
		ss = ctxStream{
			ctx:          ctx,
			ServerStream: ss,
		}
	}
	return handler(srv, ss)
}

type ctxStream struct {
	ctx context.Context
	grpc.ServerStream
}

func (ss ctxStream) Context() context.Context {
	return ss.ctx
}
