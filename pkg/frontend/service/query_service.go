// SPDX-License-Identifier: AGPL-3.0-only

package service

import (
	"context"
	"net/http"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
)

type PromCodecHandlerHTTPRoundTripperCompat interface {
	DoHandle() (querymiddleware.Response, error)
}

type PromCodecHandlerCompat struct {
	codec            querymiddleware.Codec
	promCodecHandler querymiddleware.Handler
	httpReq          *http.Request
}

func (c *PromCodecHandlerCompat) DoHandle() (querymiddleware.Response, error) {
	promCodecReq, err := c.codec.DecodeRequest(c.httpReq.Context(), c.httpReq)
	if err != nil {
		return nil, err
	}

	return c.promCodecHandler.Do(c.httpReq.Context(), promCodecReq)
}

type HTTPRoundTripperCompat struct {
	codec            querymiddleware.Codec
	httpRoundTripper http.RoundTripper
	ctx              context.Context
	promCodecReq     querymiddleware.Request

	logger log.Logger
}

func (c *HTTPRoundTripperCompat) DoHandle() (querymiddleware.Response, error) {
	httpReq, err := c.codec.EncodeRequest(c.ctx, c.promCodecReq)
	if err != nil {
		return nil, err
	}

	if err := user.InjectOrgIDIntoHTTPRequest(c.ctx, httpReq); err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	httpResp, err := c.httpRoundTripper.RoundTrip(httpReq)
	if err != nil {
		return nil, err
	}
	defer func() { _ = httpResp.Body.Close() }()

	return c.codec.DecodeResponse(c.ctx, httpResp, c.promCodecReq, c.logger)
}

type QueryService struct {
	//downstreamRoundTripper http.RoundTripper
}
