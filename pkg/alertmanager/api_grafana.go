// SPDX-License-Identifier: AGPL-3.0-only

package alertmanager

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/go-kit/log/level"
	"github.com/grafana/alerting/definition"
	"github.com/grafana/dskit/tenant"
	"github.com/pkg/errors"
	"github.com/prometheus/alertmanager/cluster/clusterpb"

	"github.com/grafana/mimir/pkg/alertmanager/alertspb"
	"github.com/grafana/mimir/pkg/util"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

const (
	errMalformedGrafanaConfigInStore = "error unmarshalling Grafana configuration from storage"
	errMarshallingState              = "error marshalling Grafana Alertmanager state"
	errMarshallingStateJSON          = "error marshalling JSON Grafana Alertmanager state"
	errMarshallingGrafanaConfigJSON  = "error marshalling JSON Grafana Alertmanager config"
	errReadingState                  = "unable to read the Grafana Alertmanager state"
	errDeletingState                 = "unable to delete the Grafana Alertmanager State"
	errStoringState                  = "unable to store the Grafana Alertmanager state"
	errReadingGrafanaConfig          = "unable to read the Grafana Alertmanager config"
	errDeletingGrafanaConfig         = "unable to delete the Grafana Alertmanager config"
	errStoringGrafanaConfig          = "unable to store the Grafana Alertmanager config"
	errBase64DecodeState             = "unable to base64 decode Grafana Alertmanager state"
	errUnmarshalProtoState           = "unable to unmarshal protobuf for Grafana Alertmanager state"

	statusSuccess = "success"
	statusError   = "error"
)

type GrafanaAlertmanagerConfig struct {
	Templates          map[string]string                    `json:"template_files"`
	AlertmanagerConfig definition.PostableApiAlertingConfig `json:"alertmanager_config"`
}
type UserGrafanaConfig struct {
	GrafanaAlertmanagerConfig GrafanaAlertmanagerConfig `json:"configuration"`
	Hash                      string                    `json:"configuration_hash"`
	CreatedAt                 int64                     `json:"created"`
	Default                   bool                      `json:"default"`
	Promoted                  bool                      `json:"promoted"`
	ExternalURL               string                    `json:"external_url"`
	StaticHeaders             map[string]string         `json:"static_headers"`
}

func (gc *UserGrafanaConfig) Validate() error {
	if gc.Hash == "" {
		return errors.New("no hash specified")
	}

	if gc.CreatedAt == 0 {
		return errors.New("created must be non-zero")
	}

	if err := gc.GrafanaAlertmanagerConfig.AlertmanagerConfig.Validate(); err != nil {
		return err
	}
	return nil
}

func (gc *UserGrafanaConfig) UnmarshalJSON(data []byte) error {
	type plain UserGrafanaConfig
	err := json.Unmarshal(data, (*plain)(gc))
	if err != nil {
		return err
	}

	if err = gc.Validate(); err != nil {
		return err
	}

	return nil
}

type UserGrafanaState struct {
	State string `json:"state"`
}

type PostableUserGrafanaState struct {
	UserGrafanaState
	Promoted bool `json:"promoted"`
}

func (gs *PostableUserGrafanaState) UnmarshalJSON(data []byte) error {
	type plain PostableUserGrafanaState
	err := json.Unmarshal(data, (*plain)(gs))
	if err != nil {
		return err
	}

	if err = gs.Validate(); err != nil {
		return err
	}

	return nil
}

func (gs *UserGrafanaState) Validate() error {
	if gs.State == "" {
		return errors.New("no state specified")
	}

	return nil
}

type successResult struct {
	Status string `json:"status"`
	Data   any    `json:"data,omitempty"`
}

type errorResult struct {
	Status string `json:"status"`
	Error  string `json:"error"`
}

func (am *MultitenantAlertmanager) GetUserGrafanaState(w http.ResponseWriter, r *http.Request) {
	logger := util_log.WithContext(r.Context(), am.logger)

	userID, err := tenant.TenantID(r.Context())
	if err != nil {
		level.Error(logger).Log("msg", errNoOrgID, "err", err.Error())
		w.WriteHeader(http.StatusUnauthorized)
		util.WriteJSONResponse(w, errorResult{Status: statusError, Error: fmt.Sprintf("%s: %s", errNoOrgID, err.Error())})
		return
	}

	st, err := am.store.GetFullGrafanaState(r.Context(), userID)
	if err != nil {
		if errors.Is(err, alertspb.ErrNotFound) {
			w.WriteHeader(http.StatusNotFound)
			util.WriteJSONResponse(w, errorResult{Status: statusError, Error: err.Error()})
		} else {
			w.WriteHeader(http.StatusInternalServerError)
			util.WriteJSONResponse(w, errorResult{Status: statusError, Error: err.Error()})
		}
		return
	}

	bytes, err := st.State.Marshal()
	if err != nil {
		level.Error(logger).Log("msg", errMarshallingState, "err", err, "user", userID)
		w.WriteHeader(http.StatusInternalServerError)
		util.WriteJSONResponse(w, errorResult{Status: statusError, Error: fmt.Sprintf("%s: %s", errMarshallingState, err.Error())})
		return
	}

	util.WriteJSONResponse(w, successResult{
		Status: statusSuccess,
		Data:   &UserGrafanaState{State: base64.StdEncoding.EncodeToString(bytes)},
	})
}

func (am *MultitenantAlertmanager) SetUserGrafanaState(w http.ResponseWriter, r *http.Request) {
	logger := util_log.WithContext(r.Context(), am.logger)
	userID, err := tenant.TenantID(r.Context())
	if err != nil {
		level.Error(logger).Log("msg", errNoOrgID, "err", err.Error())
		w.WriteHeader(http.StatusUnauthorized)
		util.WriteJSONResponse(w, errorResult{Status: statusError, Error: fmt.Sprintf("%s: %s", errNoOrgID, err.Error())})
		return
	}

	payload, err := io.ReadAll(r.Body)
	if err != nil {
		level.Error(logger).Log("msg", errReadingState, "err", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		util.WriteJSONResponse(w, errorResult{
			Status: statusError,
			Error:  fmt.Sprintf("%s: %s", errReadingState, err.Error()),
		})
		return
	}

	st := &PostableUserGrafanaState{}
	err = json.Unmarshal(payload, st)
	if err != nil {
		level.Error(logger).Log("msg", errMarshallingStateJSON, "err", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		util.WriteJSONResponse(w, errorResult{
			Status: statusError,
			Error:  fmt.Sprintf("%s: %s", errMarshallingStateJSON, err.Error()),
		})
		return
	}

	decodedBytes, err := base64.StdEncoding.DecodeString(st.State)
	if err != nil {
		level.Error(logger).Log("msg", errBase64DecodeState, "err", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		util.WriteJSONResponse(w, errorResult{
			Status: statusError,
			Error:  fmt.Sprintf("%s: %s", errBase64DecodeState, err.Error()),
		})
		return
	}

	protoState := &clusterpb.FullState{}
	err = protoState.Unmarshal(decodedBytes)
	if err != nil {
		level.Error(logger).Log("msg", errUnmarshalProtoState, "err", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		util.WriteJSONResponse(w, errorResult{
			Status: statusError,
			Error:  fmt.Sprintf("%s: %s", errUnmarshalProtoState, err.Error()),
		})
		return
	}

	err = am.store.SetFullGrafanaState(r.Context(), userID, alertspb.FullStateDesc{State: protoState})
	if err != nil {
		level.Error(logger).Log("msg", errStoringState, "err", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		util.WriteJSONResponse(w, errorResult{
			Status: statusError,
			Error:  fmt.Sprintf("%s: %s", errStoringState, err.Error()),
		})
		return
	}

	w.WriteHeader(http.StatusCreated)
	util.WriteJSONResponse(w, successResult{
		Status: statusSuccess,
	})
}

func (am *MultitenantAlertmanager) DeleteUserGrafanaState(w http.ResponseWriter, r *http.Request) {
	logger := util_log.WithContext(r.Context(), am.logger)
	userID, err := tenant.TenantID(r.Context())
	if err != nil {
		level.Error(logger).Log("msg", errNoOrgID, "err", err.Error())
		w.WriteHeader(http.StatusUnauthorized)
		util.WriteJSONResponse(w, errorResult{Status: statusError, Error: fmt.Sprintf("%s: %s", errNoOrgID, err.Error())})
		return
	}

	err = am.store.DeleteFullGrafanaState(r.Context(), userID)
	if err != nil {
		level.Error(logger).Log("msg", errDeletingState, "err", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		util.WriteJSONResponse(w, errorResult{Status: statusError, Error: fmt.Sprintf("%s: %s", errDeletingState, err.Error())})
		return
	}

	w.WriteHeader(http.StatusOK)
	util.WriteJSONResponse(w, successResult{Status: statusSuccess})
}

func (am *MultitenantAlertmanager) GetUserGrafanaConfig(w http.ResponseWriter, r *http.Request) {
	logger := util_log.WithContext(r.Context(), am.logger)

	userID, err := tenant.TenantID(r.Context())
	if err != nil {
		level.Error(logger).Log("msg", errNoOrgID, "err", err.Error())
		w.WriteHeader(http.StatusUnauthorized)
		util.WriteJSONResponse(w, errorResult{Status: statusError, Error: fmt.Sprintf("%s: %s", errNoOrgID, err.Error())})
		return
	}

	cfg, err := am.store.GetGrafanaAlertConfig(r.Context(), userID)
	if err != nil {
		if errors.Is(err, alertspb.ErrNotFound) {
			w.WriteHeader(http.StatusNotFound)
			util.WriteJSONResponse(w, errorResult{Status: statusError, Error: err.Error()})
		} else {
			w.WriteHeader(http.StatusInternalServerError)
			util.WriteJSONResponse(w, errorResult{Status: statusError, Error: err.Error()})
		}
		return
	}

	var grafanaConfig GrafanaAlertmanagerConfig
	if err := json.Unmarshal([]byte(cfg.RawConfig), &grafanaConfig); err != nil {
		level.Error(logger).Log("msg", errMalformedGrafanaConfigInStore, "err", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		util.WriteJSONResponse(w, errorResult{Status: statusError, Error: err.Error()})
		return
	}

	util.WriteJSONResponse(w, successResult{
		Status: statusSuccess,
		Data: &UserGrafanaConfig{
			GrafanaAlertmanagerConfig: grafanaConfig,
			Hash:                      cfg.Hash,
			CreatedAt:                 cfg.CreatedAtTimestamp,
			Default:                   cfg.Default,
			Promoted:                  cfg.Promoted,
			ExternalURL:               cfg.ExternalUrl,
			StaticHeaders:             cfg.StaticHeaders,
		},
	})
}

func (am *MultitenantAlertmanager) SetUserGrafanaConfig(w http.ResponseWriter, r *http.Request) {
	logger := util_log.WithContext(r.Context(), am.logger)
	userID, err := tenant.TenantID(r.Context())
	if err != nil {
		level.Error(logger).Log("msg", errNoOrgID, "err", err.Error())
		w.WriteHeader(http.StatusUnauthorized)
		util.WriteJSONResponse(w, errorResult{Status: statusError, Error: fmt.Sprintf("%s: %s", errNoOrgID, err.Error())})
		return
	}

	var input io.Reader
	maxConfigSize := am.limits.AlertmanagerMaxGrafanaConfigSize(userID)
	if maxConfigSize > 0 {
		input = http.MaxBytesReader(w, r.Body, int64(maxConfigSize))
	} else {
		input = r.Body
	}

	payload, err := io.ReadAll(input)
	if err != nil {
		if maxBytesErr := (&http.MaxBytesError{}); errors.As(err, &maxBytesErr) {
			msg := fmt.Sprintf(errConfigurationTooBig, maxConfigSize)
			level.Warn(logger).Log("msg", msg)
			w.WriteHeader(http.StatusBadRequest)
			util.WriteJSONResponse(w, errorResult{Status: statusError, Error: msg})
			return
		}

		level.Error(logger).Log("msg", errReadingGrafanaConfig, "err", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		util.WriteJSONResponse(w, errorResult{Status: statusError, Error: fmt.Sprintf("%s: %s", errReadingGrafanaConfig, err.Error())})
		return
	}

	cfg := &UserGrafanaConfig{}
	err = json.Unmarshal(payload, cfg)
	if err != nil {
		level.Error(logger).Log("msg", errMarshallingGrafanaConfigJSON, "err", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		util.WriteJSONResponse(w, errorResult{Status: statusError, Error: fmt.Sprintf("%s: %s", errMarshallingGrafanaConfigJSON, err.Error())})
		return
	}

	rawCfg, err := json.Marshal(cfg.GrafanaAlertmanagerConfig)
	if err != nil {
		level.Error(logger).Log("msg", errMarshallingGrafanaConfigJSON, "err", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		util.WriteJSONResponse(w, errorResult{Status: statusError, Error: fmt.Sprintf("%s: %s", errStoringGrafanaConfig, err.Error())})
		return
	}

	cfgDesc := alertspb.ToGrafanaProto(string(rawCfg), userID, cfg.Hash, cfg.CreatedAt, cfg.Default, cfg.Promoted, cfg.ExternalURL, cfg.StaticHeaders)
	err = am.store.SetGrafanaAlertConfig(r.Context(), cfgDesc)
	if err != nil {
		level.Error(logger).Log("msg", errStoringGrafanaConfig, "err", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		util.WriteJSONResponse(w, errorResult{Status: statusError, Error: fmt.Sprintf("%s: %s", errStoringGrafanaConfig, err.Error())})
		return
	}

	w.WriteHeader(http.StatusCreated)
	util.WriteJSONResponse(w, successResult{Status: statusSuccess})
}

func (am *MultitenantAlertmanager) DeleteUserGrafanaConfig(w http.ResponseWriter, r *http.Request) {
	logger := util_log.WithContext(r.Context(), am.logger)
	userID, err := tenant.TenantID(r.Context())
	if err != nil {
		level.Error(logger).Log("msg", errNoOrgID, "err", err.Error())
		w.WriteHeader(http.StatusUnauthorized)
		util.WriteJSONResponse(w, errorResult{Status: statusError, Error: fmt.Sprintf("%s: %s", errNoOrgID, err.Error())})
		return
	}

	err = am.store.DeleteGrafanaAlertConfig(r.Context(), userID)
	if err != nil {
		level.Error(logger).Log("msg", errDeletingGrafanaConfig, "err", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		util.WriteJSONResponse(w, errorResult{Status: statusError, Error: fmt.Sprintf("%s: %s", errDeletingGrafanaConfig, err.Error())})
		return
	}

	w.WriteHeader(http.StatusOK)
	util.WriteJSONResponse(w, successResult{Status: statusSuccess})
}
