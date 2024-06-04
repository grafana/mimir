package backend

import (
	"context"
	"net/http"

	"github.com/grafana/grafana-plugin-sdk-go/backend/httpclient"
	"github.com/grafana/grafana-plugin-sdk-go/genproto/pluginv2"
)

// dataSDKAdapter adapter between low level plugin protocol and SDK interfaces.
type dataSDKAdapter struct {
	queryDataHandler QueryDataHandler
}

func newDataSDKAdapter(handler QueryDataHandler) *dataSDKAdapter {
	return &dataSDKAdapter{
		queryDataHandler: handler,
	}
}

func withHeaderMiddleware(ctx context.Context, headers http.Header) context.Context {
	if len(headers) > 0 {
		ctx = httpclient.WithContextualMiddleware(ctx,
			httpclient.MiddlewareFunc(func(opts httpclient.Options, next http.RoundTripper) http.RoundTripper {
				if !opts.ForwardHTTPHeaders {
					return next
				}

				return httpclient.RoundTripperFunc(func(qreq *http.Request) (*http.Response, error) {
					// Only set a header if it is not already set.
					for k, v := range headers {
						if qreq.Header.Get(k) == "" {
							for _, vv := range v {
								qreq.Header.Add(k, vv)
							}
						}
					}
					return next.RoundTrip(qreq)
				})
			}))
	}
	return ctx
}

func (a *dataSDKAdapter) QueryData(ctx context.Context, req *pluginv2.QueryDataRequest) (*pluginv2.QueryDataResponse, error) {
	ctx = propagateTenantIDIfPresent(ctx)
	ctx = WithGrafanaConfig(ctx, NewGrafanaCfg(req.PluginContext.GrafanaConfig))
	parsedReq := FromProto().QueryDataRequest(req)
	ctx = withHeaderMiddleware(ctx, parsedReq.GetHTTPHeaders())
	ctx = withContextualLogAttributes(ctx, parsedReq.PluginContext, endpointQueryData)
	ctx = WithUserAgent(ctx, parsedReq.PluginContext.UserAgent)
	resp, err := a.queryDataHandler.QueryData(ctx, parsedReq)
	if err != nil {
		return nil, err
	}

	return ToProto().QueryDataResponse(resp)
}
