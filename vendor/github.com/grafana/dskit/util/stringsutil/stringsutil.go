package stringsutil

// SliceContains returns true if the search value is within the list of input values.
func SliceContains(values []string, search string) bool {
	for _, v := range values {
		if search == v {
			return true
		}
	}
	return false
}
