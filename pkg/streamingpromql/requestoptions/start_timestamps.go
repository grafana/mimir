// SPDX-License-Identifier: AGPL-3.0-only

package requestoptions

import (
	"context"
	"strconv"

	apierror "github.com/grafana/mimir/pkg/api/error"
)

// StartTimestampOption preserves whether a request overrides the engine default.
type StartTimestampOption uint8

const (
	StartTimestampsUnset StartTimestampOption = iota
	StartTimestampsDisabled
	StartTimestampsEnabled
)

func StartTimestampOptionFromBool(value *bool) StartTimestampOption {
	if value == nil {
		return StartTimestampsUnset
	}
	if *value {
		return StartTimestampsEnabled
	}
	return StartTimestampsDisabled
}

func (o StartTimestampOption) Resolve(engineDefault bool) bool {
	switch o {
	case StartTimestampsEnabled:
		return true
	case StartTimestampsDisabled:
		return false
	default:
		return engineDefault
	}
}

type startTimestampCollectionContextKey struct{}

func ParseUseStartTimestamps(value string) (*bool, error) {
	if value == "" {
		return nil, nil
	}

	enabled, err := strconv.ParseBool(value)
	if err != nil {
		return nil, apierror.Newf(apierror.TypeBadData, "invalid value %q for %s header: must be a boolean", value, UseStartTimestampsHeader)
	}

	return &enabled, nil
}

// ContextWithStartTimestampCollection carries the resolved engine setting to storage.
func ContextWithStartTimestampCollection(ctx context.Context, enabled bool) context.Context {
	return context.WithValue(ctx, startTimestampCollectionContextKey{}, enabled)
}

func CollectStartTimestampsFromContext(ctx context.Context) bool {
	enabled, _ := ctx.Value(startTimestampCollectionContextKey{}).(bool)
	return enabled
}
