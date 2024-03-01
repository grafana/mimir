package service

import "net/http"

type QueryService struct {
	roundTripper http.RoundTripper
}
