// SPDX-License-Identifier: AGPL-3.0-only

package requestoptions

import (
	"context"
	"strconv"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/util/propagation"
)

type startTimestampsContextKey struct{}

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

type StartTimestampsExtractor struct{}

func (*StartTimestampsExtractor) ExtractFromCarrier(ctx context.Context, carrier propagation.Carrier) (context.Context, error) {
	enabled, err := ParseUseStartTimestamps(carrier.Get(UseStartTimestampsHeader))
	if err != nil {
		return nil, err
	}

	return context.WithValue(ctx, startTimestampsContextKey{}, enabled != nil && *enabled), nil
}

func UseStartTimestampsFromContext(ctx context.Context) bool {
	enabled, _ := ctx.Value(startTimestampsContextKey{}).(bool)
	return enabled
}
