package v0mimir1

import (
	"net/http"

	commoncfg "github.com/prometheus/common/config"
)

func NewClientFromConfig(cfg *HTTPClientConfig, name string, optFuncs ...commoncfg.HTTPClientOption) (*http.Client, error) {
	if cfg == nil {
		cfg = &DefaultHTTPClientConfig
	}
	converted := cfg.ToCommonHTTPClientConfig()
	if err := converted.Validate(); err != nil {
		return nil, err
	}
	return commoncfg.NewClientFromConfig(*converted, name, optFuncs...)
}
