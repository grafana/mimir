// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

// AreLabelNamesSortedAndUnique returns whether the labels are sorted by their names in
// lexicographic ascending order, with each name appearing once.
func AreLabelNamesSortedAndUnique(labels []LabelAdapter) bool {
	for i := 1; i < len(labels); i++ {
		if labels[i].Name <= labels[i-1].Name {
			return false
		}
	}
	return true
}
