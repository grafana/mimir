package debug

import (
	"unicode/utf8"

	"github.com/prometheus/prometheus/model/labels"
)

func ValidatePromLabels(lbls labels.Labels) bool {
	for _, l := range lbls {
		if !utf8.ValidString(l.Name) {
			return false
		}
		if !utf8.ValidString(l.Value) {
			return false
		}
	}
	return true
}
