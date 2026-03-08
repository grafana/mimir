// SPDX-License-Identifier: AGPL-3.0-only

package labelaccess

import (
	"context"

	"github.com/grafana/mimir/pkg/util/propagation"
)

type extractor struct{}

// NewExtractor returns a new extractor for label access policies.
func NewExtractor() propagation.Extractor {
	return &extractor{}
}

// ExtractFromCarrier extracts label policies from the carrier and injects them into the context.
func (e *extractor) ExtractFromCarrier(ctx context.Context, carrier propagation.Carrier) (context.Context, error) {
	// Check if policies are already in the context (e.g., from middleware).
	// If so, don't overwrite them.
	if _, err := ExtractLabelMatchersContext(ctx); err == nil {
		// Policies already exist in context, return as-is.
		return ctx, nil
	}

	matchers, err := ExtractLabelMatchersSlice(carrier.GetAll(HTTPHeaderKey))
	if err != nil {
		return nil, err
	}

	// Only inject if we actually found matchers in the carrier.
	if len(matchers) > 0 {
		return InjectLabelMatchersContext(ctx, matchers), nil
	}

	return ctx, nil
}

type injector struct{}

// NewInjector returns a new injector for label access policies.
func NewInjector() propagation.Injector {
	return &injector{}
}

// InjectToCarrier injects label policies from the context into the carrier.
func (i *injector) InjectToCarrier(ctx context.Context, carrier propagation.Carrier) error {
	labelPolicySet, err := ExtractLabelMatchersContext(ctx)
	// If there are no matchers in the context, just return without error.
	// This is expected for queries without label policies.
	if err != nil {
		if err == errNoMatcherSource {
			return nil
		}
		return err
	}

	// If the instance policies are set on the query, ensure the LBAC configs are reinjected into the HTTP request
	// as headers. This is necessary after the querymiddleware has done its job,
	// because the RoundTrippers in querymiddleware throw away any HTTP headers they don't care about.
	if len(labelPolicySet) != 0 {
		matchers, err := InjectLabelMatchersSlice(labelPolicySet)
		if err != nil {
			return err
		}

		carrier.SetAll(HTTPHeaderKey, matchers)
	}

	return nil
}
