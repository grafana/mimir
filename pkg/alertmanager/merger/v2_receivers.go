package merger

import (
	"errors"
	"strings"
	"time"

	"github.com/go-openapi/swag"
	v2_models "github.com/prometheus/alertmanager/api/v2/models"
	"golang.org/x/exp/slices"
)

// V2Receivers implements the Merger interface for GET /v2/receivers. It returns
// the receivers and integrations that have most recent notification attempt
type V2Receivers struct{}

func (V2Receivers) MergeResponses(in [][]byte) ([]byte, error) {
	receivers := make([]v2_models.Receiver, 0)
	for _, body := range in {
		parsed := make([]v2_models.Receiver, 0)
		if err := swag.ReadJSON(body, &parsed); err != nil {
			return nil, err
		}
		receivers = append(receivers, parsed...)
	}

	merged, err := mergeV2Receivers(receivers)
	if err != nil {
		return nil, err
	}

	return swag.WriteJSON(merged)
}

func mergeV2Receivers(r []v2_models.Receiver) ([]v2_models.Receiver, error) {
	receiversByName := make(map[string]v2_models.Receiver)
	for _, receiver := range r {
		// ignore receivers without name. Although it's not possible to have a receiver without name, making sure it's not nil to avoid panic
		if receiver.Name == nil {
			return nil, errors.New("unexpected nil name")
		}
		name := *receiver.Name
		existing, ok := receiversByName[name]
		if !ok {
			receiversByName[name] = receiver
			continue
		}
		// pick receiver which integrations sent notifications more recently than others. Usually, only one Alertmanager sends notifications. Therefore, it will always win.
		if getLastNotificationAttempt(receiver).After(getLastNotificationAttempt(existing)) {
			receiversByName[name] = receiver
		}
	}
	result := make([]v2_models.Receiver, 0, len(receiversByName))
	for _, receiver := range receiversByName {
		result = append(result, receiver)
	}
	slices.SortFunc(result, func(a, b v2_models.Receiver) bool {
		// the check above guarantees that names are not nil
		return strings.Compare(*a.Name, *b.Name) < 0
	})
	return result, nil
}

func getLastNotificationAttempt(receiver v2_models.Receiver) time.Time {
	result := time.Time{}
	for _, i := range receiver.Integrations {
		t := time.Time(i.LastNotifyAttempt)
		if t.After(result) {
			result = t
		}
	}
	return result
}
