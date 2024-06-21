//go:build validatingpools

package ingester

import (
	"testing"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestMain(m *testing.M) {
	mimirpb.VerifyPools(m)
}
