// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"flag"

	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/util"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/util/tls"
)

type NotifierConfig struct {
	TLS       tls.ClientConfig `yaml:",inline"`
	BasicAuth util.BasicAuth   `yaml:",inline"`
}

func (cfg *NotifierConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.TLS.RegisterFlagsWithPrefix("ruler.alertmanager-client", f)
	cfg.BasicAuth.RegisterFlagsWithPrefix("ruler.alertmanager-client.", f)
}
