/*
 * Copyright The OpenTelemetry Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package builder

import "math"

// Dictionary is a configuration for a dictionary field.
// The MaxCard is the maximum cardinality of the dictionary field. If the
// cardinality of the dictionary field is higher than MaxCard, then the
// dictionary field will be automatically converted to its base type.
//
// if MaxCard is equal to 0, then the dictionary field will be converted to its
// base type no matter what.
type Dictionary struct {
	MinCard        uint64
	MaxCard        uint64
	ResetThreshold float64
}

// NewDictionary creates a new dictionary configuration with the given maximum
// cardinality.
func NewDictionary(maxCard uint64, resetThreshold float64) *Dictionary {
	// If `maxCard` is 0 (no dictionary configuration), then the dictionary
	// field will be converted to its base type no matter what. So, the minimum
	// cardinality will be set to 0.
	minCard := uint64(math.MaxUint8)
	if maxCard < minCard {
		minCard = maxCard
	}
	return &Dictionary{
		MinCard:        minCard,
		MaxCard:        maxCard,
		ResetThreshold: resetThreshold,
	}
}

// NewDictionaryFrom creates a new dictionary configuration from a prototype
// dictionary configuration with the given minimum cardinality.
func NewDictionaryFrom(minCard uint64, dicProto *Dictionary) *Dictionary {
	// If `maxCard` is 0 (no dictionary configuration), then the dictionary
	// field will be converted to its base type no matter what. So, the minimum
	// cardinality will be set to 0.
	if dicProto.MaxCard < minCard {
		minCard = dicProto.MaxCard
	}
	return &Dictionary{
		MinCard:        minCard,
		MaxCard:        dicProto.MaxCard,
		ResetThreshold: dicProto.ResetThreshold,
	}
}
