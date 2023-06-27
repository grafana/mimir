package notify

import (
	"errors"
	"fmt"
	"time"

	"github.com/go-kit/log/level"
	v2 "github.com/prometheus/alertmanager/api/v2"
	amv2 "github.com/prometheus/alertmanager/api/v2/models"
	"github.com/prometheus/alertmanager/silence"
)

var (
	ErrGetSilencesInternal     = fmt.Errorf("unable to retrieve silence(s) due to an internal error")
	ErrDeleteSilenceInternal   = fmt.Errorf("unable to delete silence due to an internal error")
	ErrCreateSilenceBadPayload = fmt.Errorf("unable to create silence")
	ErrListSilencesBadPayload  = fmt.Errorf("unable to list silences")
	ErrSilenceNotFound         = silence.ErrNotFound
)

type GettableSilences = amv2.GettableSilences
type GettableSilence = amv2.GettableSilence
type PostableSilence = amv2.PostableSilence

type Silence = amv2.Silence

// ListSilences retrieves a list of stored silences. It supports a set of labels as filters.
func (am *GrafanaAlertmanager) ListSilences(filter []string) (GettableSilences, error) {
	matchers, err := parseFilter(filter)
	if err != nil {
		level.Error(am.logger).Log("msg", "failed to parse matchers", "err", err)
		return nil, fmt.Errorf("%s: %w", ErrListSilencesBadPayload.Error(), err)
	}

	psils, _, err := am.silences.Query()
	if err != nil {
		level.Error(am.logger).Log("msg", ErrGetSilencesInternal.Error(), "err", err)
		return nil, fmt.Errorf("%s: %w", ErrGetSilencesInternal.Error(), err)
	}

	sils := GettableSilences{}
	for _, ps := range psils {
		if !v2.CheckSilenceMatchesFilterLabels(ps, matchers) {
			continue
		}
		silence, err := v2.GettableSilenceFromProto(ps)
		if err != nil {
			level.Error(am.logger).Log("msg", "unmarshaling from protobuf failed", "err", err)
			return GettableSilences{}, fmt.Errorf("%s: failed to convert internal silence to API silence: %w",
				ErrGetSilencesInternal.Error(), err)
		}
		sils = append(sils, &silence)
	}

	v2.SortSilences(sils)

	return sils, nil
}

// GetSilence retrieves a silence by the provided silenceID. It returns ErrSilenceNotFound if the silence is not present.
func (am *GrafanaAlertmanager) GetSilence(silenceID string) (GettableSilence, error) {
	sils, _, err := am.silences.Query(silence.QIDs(silenceID))
	if err != nil {
		return GettableSilence{}, fmt.Errorf("%s: %w", ErrGetSilencesInternal.Error(), err)
	}

	if len(sils) == 0 {
		level.Error(am.logger).Log("msg", "failed to find silence", "err", err, "id", sils)
		return GettableSilence{}, ErrSilenceNotFound
	}

	sil, err := v2.GettableSilenceFromProto(sils[0])
	if err != nil {
		level.Error(am.logger).Log("msg", "unmarshaling from protobuf failed", "err", err)
		return GettableSilence{}, fmt.Errorf("%s: failed to convert internal silence to API silence: %w",
			ErrGetSilencesInternal.Error(), err)
	}

	return sil, nil
}

// CreateSilence persists the provided silence and returns the silence ID if successful.
func (am *GrafanaAlertmanager) CreateSilence(ps *PostableSilence) (string, error) {
	sil, err := v2.PostableSilenceToProto(ps)
	if err != nil {
		level.Error(am.logger).Log("msg", "marshaling to protobuf failed", "err", err)
		return "", fmt.Errorf("%s: failed to convert API silence to internal silence: %w",
			ErrCreateSilenceBadPayload.Error(), err)
	}

	if sil.StartsAt.After(sil.EndsAt) || sil.StartsAt.Equal(sil.EndsAt) {
		msg := "start time must be before end time"
		level.Error(am.logger).Log("msg", msg, "err", "starts_at", sil.StartsAt, "ends_at", sil.EndsAt)
		return "", fmt.Errorf("%s: %w", msg, ErrCreateSilenceBadPayload)
	}

	if sil.EndsAt.Before(time.Now()) {
		msg := "end time can't be in the past"
		level.Error(am.logger).Log("msg", msg, "ends_at", sil.EndsAt)
		return "", fmt.Errorf("%s: %w", msg, ErrCreateSilenceBadPayload)
	}

	silenceID, err := am.silences.Set(sil)
	if err != nil {
		level.Error(am.logger).Log("msg", "unable to save silence", "err", err)
		if errors.Is(err, silence.ErrNotFound) {
			return "", ErrSilenceNotFound
		}
		return "", fmt.Errorf("unable to save silence: %s: %w", err.Error(), ErrCreateSilenceBadPayload)
	}

	return silenceID, nil
}

// DeleteSilence looks for and expires the silence by the provided silenceID. It returns ErrSilenceNotFound if the silence is not present.
func (am *GrafanaAlertmanager) DeleteSilence(silenceID string) error {
	if err := am.silences.Expire(silenceID); err != nil {
		if errors.Is(err, silence.ErrNotFound) {
			return ErrSilenceNotFound
		}
		return fmt.Errorf("%s: %w", err.Error(), ErrDeleteSilenceInternal)
	}

	return nil
}
