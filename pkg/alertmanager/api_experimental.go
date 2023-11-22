// SPDX-License-Identifier: AGPL-3.0-only

package alertmanager

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/pkg/errors"
	"github.com/prometheus/alertmanager/cluster/clusterpb"

	"github.com/grafana/mimir/pkg/alertmanager/alertspb"
	"github.com/grafana/mimir/pkg/util"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

const (
	errMarshallingState             = "error marshalling Grafana Alertmanager state"
	errMarshallingStateJSON         = "error marshalling JSON Grafana Alertmanager state"
	errMarshallingGrafanaConfigJSON = "error marshalling JSON Grafana Alertmanager config"
	errReadingState                 = "unable to read the Grafana Alertmanager state"
	errDeletingState                = "unable to delete the Grafana Alertmanager State"
	errStoringState                 = "unable to store the Grafana Alertmanager state"
	errReadingGrafanaConfig         = "unable to read the Grafana Alertmanager config"
	errDeletingGrafanaConfig        = "unable to delete the Grafana Alertmanager config"
	errStoringGrafanaConfig         = "unable to store the Grafana Alertmanager config"
	errBase64DecodeState            = "unable to base64 decode Grafana Alertmanager state"
	errUnmarshalProtoState          = "unable to unmarshal protobuf for Grafana Alertmanager state"

	statusSuccess = "success"
	statusError   = "error"
)

type UserGrafanaConfig struct {
	ID                        int64  `json:"id"`
	GrafanaAlertmanagerConfig string `json:"configuration"`
	Hash                      string `json:"configuration_hash"`
	CreatedAt                 int64  `json:"created"`
	Default                   bool   `json:"default"`
}

func (gc *UserGrafanaConfig) Validate() error {
	if gc.GrafanaAlertmanagerConfig == "" {
		return errors.New("no Grafana Alertmanager config specified")
	}

	return nil
}

type UserGrafanaState struct {
	State string `json:"state"`
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

	// TODO: Extract an issue to limit the number of bytes we should read.
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

	st := &UserGrafanaState{}
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

	util.WriteJSONResponse(w, successResult{
		Status: statusSuccess,
		Data: &UserGrafanaConfig{
			ID:                        cfg.Id,
			GrafanaAlertmanagerConfig: cfg.RawConfig,
			Hash:                      cfg.Hash,
			CreatedAt:                 cfg.CreatedAt,
			Default:                   cfg.Default,
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

	// TODO: Extract issue, we need to enforce a limit that checks against both configs at the same time.
	payload, err := io.ReadAll(r.Body)
	if err != nil {
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

	cfgDesc := alertspb.ToGrafanaProto(cfg.GrafanaAlertmanagerConfig, userID, cfg.Hash, cfg.ID, cfg.CreatedAt, cfg.Default)
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
