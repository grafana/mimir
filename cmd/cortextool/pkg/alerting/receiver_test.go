package alerting

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/gorilla/mux"
	"github.com/prometheus/alertmanager/template"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

func Test_measureLatency(t *testing.T) {
	tc := []struct {
		name    string
		alerts  template.Data
		err     error
		tracked []float64
	}{
		{
			name: "with alerts to track",
			alerts: template.Data{
				Alerts: template.Alerts{
					template.Alert{
						Labels:      template.KV{model.AlertNameLabel: "e2ealertingAlwaysFiring"},
						Annotations: template.KV{"time": "1.604069614e+09"},
						Status:      string(model.AlertFiring),
					},
					template.Alert{
						Labels:      template.KV{model.AlertNameLabel: "e2ealertingAlwaysFiring"},
						Annotations: template.KV{"time": "1.604069615e+09"},
						Status:      string(model.AlertFiring),
					},
				},
			},
			tracked: []float64{1604069614.00, 1604069615.00},
		},
		{
			name: "with alerts that don't have a time annotation or alertname label it ignores them",
			alerts: template.Data{
				Alerts: template.Alerts{
					template.Alert{
						Labels:      template.KV{model.AlertNameLabel: "e2ealertingAlwaysFiring"},
						Annotations: template.KV{"time": "1.604069614e+09"},
						Status:      string(model.AlertFiring),
					},
					template.Alert{
						Labels: template.KV{model.AlertNameLabel: "e2ealertingAlwaysFiring"},
						Status: string(model.AlertFiring),
					},
					template.Alert{
						Annotations: template.KV{"time": "1.604069614e+09"},
						Status:      string(model.AlertFiring),
					},
				},
			},
			tracked: []float64{1604069614.00},
		},
	}

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			r, err := NewReceiver(
				ReceiverConfig{PurgeInterval: 1 * time.Hour},
				log.NewNopLogger(),
				prometheus.NewRegistry(),
			)
			require.NoError(t, err)

			router := mux.NewRouter()
			r.RegisterRoutes(router)

			b, err := json.Marshal(tt.alerts)
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodPost, "/api/v1/receiver", bytes.NewBuffer(b))
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			require.Equal(t, http.StatusOK, w.Code)
			require.Equal(t, len(tt.tracked), len(r.timestamps))
			for _, timestamp := range tt.tracked {
				_, exists := r.timestamps[timestamp]
				require.True(t, exists, fmt.Sprintf("time %f is not tracked", timestamp))
			}
		})
	}
}
