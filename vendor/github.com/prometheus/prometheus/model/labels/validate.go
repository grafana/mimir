// Copyright 2025 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package labels

import "github.com/prometheus/common/model"

// IsValidMetricName returns whether name is a valid metric name, depending on the validation scheme.
func IsValidMetricName(name string, scheme model.ValidationScheme) bool {
	if scheme == model.LegacyValidation {
		return model.IsValidLegacyMetricName(name)
	}
	return model.IsValidMetricName(model.LabelValue(name))
}

// IsValidLabelName returns whether name is a valid label name, depending on the validation scheme.
func IsValidLabelName(name string, scheme model.ValidationScheme) bool {
	if scheme == model.LegacyValidation {
		return model.LabelName(name).IsValidLegacy()
	}
	return model.LabelName(name).IsValid()
}
